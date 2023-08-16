#!/usr/bin/env Rscript

# load packages
suppressMessages(library(tidyverse))
suppressMessages(library(tidymodels))
suppressMessages(library(furrr))
suppressMessages(library(slider))
suppressMessages(library(arrow))
suppressMessages(library(GGIRread))
suppressMessages(library(glue))
suppressMessages(library(lubridate))
suppressMessages(library(signal))
suppressMessages(library(moments))

args <- R.utils::commandArgs(trailingOnly = TRUE)

# epoch_length <- as.integer(args[1]) # epoch length in seconds
path <- args[1] # path to accelerometer files
file_type <- args[2]  

# seconds
epoch_length <- 30

files_basenames <-
  list.files(path, full.names = F) %>%
  str_subset(file_type)

files <-
  str_c(path, files_basenames)

temp_files <-
  str_replace_all(str_c(path, "temp/", files_basenames), str_c(".", file_type), ".parquet")

# The mean_crossing_rate function takes the signal as input and calculates
# the mean crossing rate. It counts the number of times the signal crosses
# the mean value and divides it by the length of the signal.
mean_crossing_rate <- function(signal) {
  crossings <- sum(abs(diff(sign(signal))) / 2)
  crossing_rate <- crossings / length(signal)
  return(crossing_rate)
}

cat("\n Applying filter and calculating the first round of features.\n 
    Progress bar will appear shortly\n 
    This will take some time...\n")

process_file <- function(file, temp_file) {
  if (file_type == "cwa") {
    # Processing specific to cwa files
    glue::glue("Extracting features for {file}...\n")

    raw <-
      GGIRread::readAxivity(file, start = 1, end = 14400)

    acc <-
      raw$data %>%
      as_tibble() %>%
      mutate(
        datetime = as.POSIXct(time, origin = "1970-01-01"),
        .before = 1
      )

    sf <-
      raw$header$frequency
  } else if (file_type == "wav") {
    # Processing specific to wav files
    glue::glue("Extracting features for {file}...\n")

    raw <- GGIRread::readWav(file, start = 1, end = 14400, units = "minutes")

    timestamp <- raw$timestamp

    temp <- rep(raw$temp, each = 30)

    sf <- 30

    n <- nrow(raw$rawxyz)

    acc <-
      raw$rawxyz %>%
      as_tibble() %>%
      mutate(
        datetime = seq(
          from = as.POSIXct(timestamp),
          by = 1 / sf,
          length.out = n
        ),
        temp = temp,
        .before = 1
      )
  }


  # Create 4th order Butterworth low-pass 5 Hz filter
  bf <- signal::butter(4, 5 / (sf / 2), type = "low")

  out <-
    acc %>%
    mutate(
      epoch = floor_date(datetime, "30 seconds"),
      noon_day = day(epoch - hours(12)),
      month = month(epoch)
    ) %>%
    group_by(epoch, noon_day, month) %>%
    reframe(
      id = str_extract(file, "\\d{10}"),
      id = str_remove(id, "^0+"),
      sensor_code = str_extract(file, "\\d{5}"),
      across(x:z, ~ signal::filtfilt(bf, .x)),
      across(c(x, y , z, temp), list(
        mean = mean,
        sd = sd
      )),
      weekday = wday(datetime, label = FALSE, week_start = 1),
      incl = 180 / pi * acos(y_mean / sqrt(x_mean^2 + y_mean^2 + z_mean^2)),
      theta = 180 / pi * asin(z_mean / sqrt(x_mean^2 + y_mean^2 + z_mean^2)),
      vector_magnitude = sqrt(x_mean^2 + y_mean^2 + z_mean^2),
      across(c(x, y, z), list(
        crossing_rate = mean_crossing_rate,
        skewness = skewness,
        kurtosis = kurtosis
      ))
    ) %>%
    group_by(epoch) %>%
    slice(1) %>%
    ungroup() %>%
    write_parquet(temp_file)
}

# plan(multisession, workers = 5)
# 
# future_walk2(files, temp_files, ~ process_file(.x, .y),
#   .options = furrr_options(seed = 123), .progress = TRUE
# )
# 
# plan(sequential)

walk2(files, temp_files, ~ process_file(.x, .y), .progress = TRUE)