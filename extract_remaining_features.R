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
    rowwise() %>%
    mutate(
      sd_max = max(c(x_sd, y_sd, z_sd))
    ) %>%
    group_by(id, noon_day, month) %>%
    mutate(
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
      seconds_since_peak = hour(epoch) * 3600 + minute(epoch) * 60 + second(epoch) - (21 * 3600), # sets c-process peak at 19:00
      clock_proxy_cos = cos(2 * pi * seconds_since_peak / (24 * 3600)),
      weekday = wday(epoch - hours(12), label = FALSE, week_start = 1),
      is_weekend = if_else(weekday %in% c(5, 6), 1, 0), # fri and sat night
      .after = sd_max
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
