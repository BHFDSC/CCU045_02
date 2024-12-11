#*******************************************************************************
#
# Project: CCU005_04
# Title:   Trends and impact of multiple long-term conditions in heart failure
# Date:    20-Mar-2023
# Author:  Robert Fletcher
# Purpose: Run pipeline
#
#*******************************************************************************


# Files to run ------------------------------------------------------------

files <- list.files(here::here("code"))
pl <- files[!grepl("0[01]", files)]


# Run pipeline ------------------------------------------------------------

for (p in 1:length(pl)) {
  source(here::here("code", pl[p]))
}
