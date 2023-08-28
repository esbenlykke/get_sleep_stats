#!/usr/bin/env Rscript

suppressMessages(library(tidyverse))
suppressMessages(library(tidymodels))
suppressMessages(library(furrr))
suppressMessages(library(arrow))
suppressMessages(library(R.utils))
suppressMessages(library(glue))
suppressMessages(library(slider))


conflicted::conflict_prefer("filter", "dplyr")
conflicted::conflict_prefer("lag", "dplyr")

args <- commandArgs(trailingOnly = TRUE)

temp_path <- args[1] # directory with temp parquet files
dest <- args[2] # destination filename

xgb_in_bed <-
  read_rds("models/xgb_in_bed_fit.rds")

xgb_sleep <-
  read_rds("models/xgb_sleep_fit.rds")

cat("Calculating remaining features\n")

basenames <- list.files(temp_path, ".parquet", full.names = FALSE)

temp_files <-
  list.files(temp_path, ".parquet", full.names = TRUE)

output_files <-
  file.path(dest, basenames)


process_temp_parquet <- function(temp_file, output_file) {
  cat(glue("Processing {str_remove(temp_file, temp_path)}...\n\n"))
  tbl <-
    read_parquet(temp_file) %>%
    mutate(age = 8) %>% # TODO: remove this fake age when real data is available
    rowwise() %>%
    mutate(
      sd_max = max(c(x_sd, y_sd, z_sd))
    ) %>%
    group_by(id, noon_day, month) %>%
    mutate(
      clock_group = if_else((hms::as_hms(epoch) > lubridate::hms("19:00:00") |
        hms::as_hms(epoch) < lubridate::hms("10:00:00")), 1, 0),
      across(c(x, y, z), list(sd_long = ~ slider::slide_dbl(.x, sd, .after = 30))),
      across(x_sd_long:z_sd_long, ~ replace_na(.x, mean(.x, na.rm = TRUE))),
      across(c(incl, theta, temp_mean, x_sd, y_sd, z_sd), list(
        lag_1min = ~ lag(.x, 2, default = mean(.x)), # Value from 1 minute ago
        lag_5min = ~ lag(.x, 10, default = mean(.x)), # Value from 5 minutes ago
        lag_30min = ~ lag(.x, 60, default = mean(.x)), # Value from 30 minutes ago
        lead_1min = ~ lead(.x, 2, default = mean(.x)), # Value from 1 minute in the future
        lead_5min = ~ lead(.x, 10, default = mean(.x)), # Value from 5 minutes in the future
        lead_30min = ~ lead(.x, 60, default = mean(.x)) # Value from 30 minutes in the future
      )),
      .after = sd_max
    ) %>%
    # Group the data by id, noon_day, month, and clock_group
    group_by(id, noon_day, month, clock_group) %>%
    # Create new features for clock_proxy_cos and clock_proxy_linear
    mutate(
      clock_proxy_cos = if_else(clock_group == 1,
        cos(seq(-(pi / 2), pi / 2, length.out = n())), 0
      ),
      clock_proxy_linear = if_else(clock_group == 1,
        seq(0, 1, length.out = n()), 0
      )
    ) %>%
    ungroup() 

  in_bed_extract <-
    xgb_in_bed %>%
    augment(tbl) %>%
    mutate(.pred_class = slide_dbl(as.numeric(.pred_class) - 1, median, .after = 15, .before = 15)) %>%
    group_by(id, noon_day, month) %>%
    group_modify(~ .x %>%
      # Filter rows where row_number is within the in_bed_filtered range
      filter(row_number() >= min(row_number()[.data$.pred_class == 1]) &
        row_number() <= max(row_number()[.data$.pred_class == 1]))) %>%
    ungroup() %>%
    rename(pred_in_bed = .pred_class, pred_in_bed_yes = .pred_1, pred_in_bed_no = .pred_0)

  combined_predictions <-
    xgb_sleep %>%
    augment(in_bed_extract) %>%
    rename(pred_asleep = .pred_class, pred_asleep_yes = .pred_1, pred_asleep_no = .pred_0) %>%
    write_parquet(output_file)
}

suppressWarnings(
  walk2(temp_files, output_files, ~ process_temp_parquet(.x, .y), .progress = TRUE)
)
# cat(glue("\n{length(warnings())} warnings generated. Warnings are generated when no in-bed time is found on a given night...\n"))
