#!/usr/bin/env Rscript

# List of required packages
required_packages <- c(
  "tidyverse", "tidymodels", "furrr", "slider", "arrow",
  "read.cwa", "glue", "lubridate", "signal", "moments", "xgboost"
)

# Check and install missing packages
missing_packages <- setdiff(required_packages, installed.packages()[, "Package"])

if (length(missing_packages) > 0) {
  install.packages(missing_packages, dependencies = TRUE)
} else {
  cat("Lets go!\n")
}