#*******************************************************************************
#
# Project: CCU005_04
# Title:   Trends and impact of multiple long-term conditions in heart failure
# Date:    13-Mar-2024
# Author:  Robert Fletcher
# Purpose: Join data
#
#*******************************************************************************


# Notes -------------------------------------------------------------------

# Open the `nicor` RStudio project to run this code


# Load libraries ----------------------------------------------------------

libs <- c("data.table", "here", "lubridate", "tidyverse")

for (l in libs) library(l, character.only = TRUE)


# Source functions --------------------------------------------------------

fns <- c("load_data_locally", "save")

for (f in fns) source(here::here("src", paste0(f, ".R")))


# Define variables --------------------------------------------------------

# Pipeline version
ver <- "v2"


# Load data ---------------------------------------------------------------

# Pre-processed data
output <- list(bl = "baseline", dt = "deaths")
for (o in 1:length(output)) {
  assign(
   names(output)[o], 
    load_data_locally(.from = "output", .table = output[[o]])
  )
}

# Medications and hospitalisations
data <- list(med = "medications", hp = "cohort_hospitalisations")
for (d in 1:length(data)) {
  assign(
    names(data)[d], 
    load_data_locally(.from = "data", .ver = ver, .table = data[[d]])
  )
}


# Join and write data -----------------------------------------------------

ad <- 
  purrr::reduce(list(bl, dt, med, hp), left_join, by = join_by(subject_id)) |>
  save("analysis_data")

