#!/usr/bin/env Rscript

suppressMessages(library(tidyverse))
suppressMessages(library(arrow))

args <- commandArgs(trailingOnly = TRUE)

preds_path <- args[1] # directory with prediction parquet files
dest <- args[2] # destination filename

basenames <-
  list.files(preds_path, ".parquet")

preds_files <-
  file.path(preds_path, basenames)

output_files <-
  file.path(dest, str_replace_all(basenames, ".parquet", ".rds"))

create_stats <- function(preds_file, output_file) {
  preds <- read_parquet(preds_file)

  id <- preds %>%
    distinct(id) %>%
    pull()
  
  # dates <- preds %>%
  #   distinct(date(epoch)) %>%
  #   pull()

  to_bed <- preds %>%
    group_by(noon_day, month) %>%
    slice(1) %>% 
    pull(epoch)

  out_bed <- preds %>%
    group_by(noon_day, month) %>%
    slice(n()) %>% 
    pull(epoch)

  stats <-
    preds %>%
    mutate(pred_asleep = as.numeric(pred_asleep) - 1) %>%
    group_by(id, noon_day, month) %>%
    summarise(
      spt_hrs = n() * 30 / 60 / 60,
      tst_hrs = sum(pred_asleep, na.rm = TRUE) * 30 / 60 / 60,
      se_percent = ifelse(spt_hrs != 0, 100 * (tst_hrs / spt_hrs), NA),
      .groups = "drop"
    ) %>%
    select(spt_hrs:se_percent)
  
  res <- list(id = id, in_bed_times = tibble(to_bed = to_bed, out_bed = out_bed), stats = stats)
  
  write_rds(res, output_file)
}

cat("Extracting sleep quality metrics... \n")

walk2(preds_files, output_files, ~ create_stats(.x, .y))
