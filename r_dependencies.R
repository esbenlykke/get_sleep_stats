#!/usr/bin/env Rscript

# List of required packages from CRAN
required_packages <- c(
  "tidyverse", "tidymodels", "slider", "arrow", "glue", "lubridate", "signal", "moments", "xgboost"
)

# Check and install missing CRAN packages
missing_packages <- setdiff(required_packages, installed.packages()[, "Package"])

if (length(missing_packages) > 0) {
  install.packages(missing_packages, dependencies = TRUE)
}

# Check and install GGIRread from GitHub if not installed
if (!"GGIRread" %in% installed.packages()[, "Package"]) {
  if (!"remotes" %in% installed.packages()[, "Package"]) {
    install.packages("remotes")
  }
  remotes::install_github("jbrond/GGIRread")
} else {
  cat("Lets go!\n")
}
