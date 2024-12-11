#*******************************************************************************
#
# Project: CCU005_04
# Title:   Trends and impact of multiple long-term conditions in heart failure
# Date:    13-Mar-2024
# Author:  Robert Fletcher
# Purpose: Examine longitudinal trends in heart failure cases and outcomes
#
#*******************************************************************************


# Notes -------------------------------------------------------------------

# Open the `nicor` RStudio project to run this code


# Load libraries ----------------------------------------------------------

libs <- c("lubridate", "tidyverse", "bit64")

for (l in libs) library(l, character.only = TRUE)


# Define variables --------------------------------------------------------

# Pipeline version
ver <- "v2"

# Today's date
date <- tolower(format(Sys.Date(), "%d%b%Y"))


# Create new sub-directories ----------------------------------------------

dir.create(here::here("figs", "medication_trends"), showWarnings = FALSE)


# Source functions --------------------------------------------------------

fns <- c("load_data_locally", "rnd", "save", "get_meds")

for (f in fns) source(here::here("src", paste0(f, ".R")))


# Load data ---------------------------------------------------------------

# Analysis data
ad <- load_data_locally(.from = "output", .table = "analysis_data")


# Code outcomes -----------------------------------------------------------

# Composite for all-cause mortality and all-cause emergency re-hospitalisation
ad2 <- ad |>
  # Remove patients with death date before index admission, patients that
  # died during their very first hospitalisation for heart failure, and patients
  # with a history of heart failure
  filter(error == 0, hhf == 0, !(in_hosp_death == 1 & hhf == 0)) |>
  dplyr::select(
    subject_id, admission_date, hf_fct, age, sex, ethnic, imd_fct, hhf, egfr,
    hckd, matches("6mo")
  ) |>
  mutate(
    # Create GFR categories
    egfr_grp = case_when(
      egfr >= 60 ~ 1,
      egfr < 60 & egfr >= 30 ~ 2,
      egfr < 30 ~ 3,
      .default = NA
    ),
    egfr_grp = factor(
      egfr_grp, levels = c(1:3), 
      labels = c("60 or more", "30 to less than 60", "Less than 30")
    ),
    egfr_60 = case_when(
      egfr >= 60 ~ 1,
      egfr < 60 ~ 2,
      .default = NA
    ),
    egfr_60 = factor(
      egfr_60, levels = c(1:2), 
      labels = c("60 or more", "Less than 60")
    ),
    # Create age group variable
    age_grp = case_when(
      age < 70 ~ 1,
      age >= 70 & age < 80 ~ 2,
      age >= 80 & age < 90 ~ 3,
      age >= 90 ~ 4,
      .default = NA
    ),
    age_grp = factor(
      age_grp, levels = c(1:4), 
      labels = c("Less than 70", "70 to 79", "80 to 89", "90 or more")
    ),
    # Recode history of heart failure
    hhf = factor(hhf, levels = c(0:1), labels = c("No", "Yes")),
    # Recode ethnicity
    ethnic = fct_collapse(
      ethnic, white = "white",
      nonwhite = c("black", "asian", "mixed", "other"),
      missing = "missing"
    ),
    # Recode index of multiple deprivation
    imd_fct = fct_recode(
      imd_fct, Deprived = "1 (Most deprived)", Two = "2", Three = "3", 
      Four = "4", Privileged = "5 (Least deprived)"
    ),
    # Recode dates of admission
    date_month = lubridate::floor_date(admission_date, unit = "month"),
    date_month = factor(date_month),
    date_year = lubridate::floor_date(admission_date, unit = "year"),
    date_year = factor(date_year),
    # Medications stuff
    across(matches("6mo_after"), \(x) if_else(x == 0, 0, as.integer(x / 1))),
    ACE_ARB_6mo_after = case_when(
      if_any(c(ACEI_6mo_after, ARB_6mo_after), \(x) x == 1) ~ 1,
      .default = 0
    ),
    RAS_6mo_after = case_when(
      if_any(c(ACEI_6mo_after, ARB_6mo_after), \(x) x == 1) ~ 1,
      .default = 0
    ),
    QUAD_6mo_after = case_when(
      if_all(
        c(SGLT2I_6mo_after, RAS_6mo_after, MRA_6mo_after, BETABLOCK_6mo_after), 
        \(x) x == 1
      ) ~ 1,
      .default = 0
    ),
    CONVENTIONAL_6mo_after = case_when(
      if_all(c(ACE_ARB_6mo_after, BETABLOCK_6mo_after), \(x) x == 1) ~ 1,
      .default = 0
    ),
    COMPREHENSIVE_6mo_after = case_when(
      if_all(
        c(SGLT2I_6mo_after, ARNI_6mo_after, MRA_6mo_after, BETABLOCK_6mo_after), 
        \(x) x == 1
      ) ~ 1,
      .default = 0
    ),
    # Center age
    age_mean = mean(age), 
    age_c = age - age_mean,
    .keep = "unused"
  )


# Summarise medication usage ----------------------------------------------

# Overall
meds_overall <- get_meds(ad2)

# By subgroup
param <- list(
  hf_fct = "hf_fct", age_grp = "age_grp", sex = "sex", imd_fct = "imd_fct", 
  ethnic = "ethnic", egfr_grp = "egfr_grp", hckd = "hckd",
  hf_fct_gfr = c("hf_fct", "egfr_grp"), hf_fct_gfr60 = c("hf_fct", "egfr_60"),
  hf_fct_ckd = c("hf_fct", "hckd")
)
out <- list()
for (m in 1:length(param)) {
  md <- get_meds(ad2, param[[m]])
  out[[m]] <- md
}


# Write data --------------------------------------------------------------

# Remove existing files in the output directory
file.remove(
  list.files(here::here("figs", "medication_trends"), full.names = TRUE)
)

export_table <- function(.table, .dir, .name) {
  # Specify ordering of files
  files <- length(list.files(here::here("figs", .dir)))
  if (files < 9) {
    num <- paste0("00", files + 1, "_")
  } else if (files >= 9 & files < 99) {
    num <- paste0("0", files + 1, "_")
  } else {
    num <- paste0(files + 1, "_")
  }
  
  # File name
  fn <- paste0(num, .name, ".csv")
  # Write
  write_csv(.table, here::here("figs", .dir, fn))
}

# Incidence
med <- c(list(meds_overall), out)
names <- paste0(c("overall", names(param)), "_med_trends")

for (m in seq_along(med)) {
  export_table(med[[m]], "medication_trends", names[m])
}
