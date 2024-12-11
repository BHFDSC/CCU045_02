#*******************************************************************************
#
# Project: CCU005_04
# Title:   Trends and impact of multiple long-term conditions in heart failure
# Date:    12-Mar-2024
# Author:  Robert Fletcher
# Purpose: Prepare deaths data
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

# COVID-19 death
covid_death <- "U071"

# Cardiovascular disease
cv_rgx <- "^I|^Q2"
# Heart failure
hf_rgx <- "^I110$|^I13[02]|^I255$|^I42[09]$|^I50"
# Myocardial infarction
mi_rgx <- "^I2[1-3][0-9]"
# Stroke
sk_rgx <- "^(G45[0-9]?|G46[0-2]?|I6[0-6][0-9]?|I64)$"
# Kidney failure
kd_rgx <- "^N17|^N18|^N19"
# Cancer
cn_rgx <- "^C"
# Neurological disease
ne_rgx <- "^F00|^F01|^F02|^F03|^F051|^G20|^G30|R54"
# Asthma
as_rgx <- "^J4[56]"
# Diabetes
db_rgx <- "^E10|^E11|^E12|^E13|^E14"
# Infection
in_rgx <- "^A|^B|^J09|^J1[0-9]|^J2[0-9]|^J3[0-9]|^J4[0-9]|^N390"
# Injury
ij_rgx <- "^V|^W"
# Autoimmune disease
ai_rgx <- "^M3"


# Load data ---------------------------------------------------------------

# Baseline data
bl <- load_data_locally(.from = "output", .table = "baseline")

# Deaths
dt <- load_data_locally(.from = "data", .ver = ver, .table = "cohort_deaths")


# Clean data --------------------------------------------------------------

# Subject admission and discharge dates
adm <- bl |>
  select(subject_id, admission_date, discharge_date) |>
  distinct()

# Prepare primary causes of death and QC for patients with deaths before index
# date
dt2 <- dt |>
  select(subject_id, death_date, primary_death_code) |>
  group_by(subject_id) |>
  fill(primary_death_code, .direction = "updown") |>
  ungroup() |>
  distinct() |>
  right_join(adm, by = join_by(subject_id)) |>
  mutate(
    error = case_when(
      death_date < admission_date ~ 1,
      .default = 0
    ),
    death_date = case_when(
      death_date >= admission_date & death_date < discharge_date ~ discharge_date,
      .default = death_date
    )
  ) |> 
  group_by(subject_id) |>
  slice_max(death_date) |>
  ungroup()

# Define endpoints
dt3 <- dt2 |>
  mutate(
    acm_ep = case_when(
      !is.na(death_date) & death_date >= discharge_date ~ 1,
      .default = 0
    ),
    cvd_ep = case_when(
      acm_ep == 1 & str_detect(primary_death_code, cv_rgx) ~ 1,
      .default = 0
    ),
    non_cvd_ep = case_when(
      acm_ep == 1 & cvd_ep == 0 ~ 1,
      .default = 0
    ),
    hfd_ep = case_when(
      acm_ep == 1 & str_detect(primary_death_code, hf_rgx) ~ 1,
      .default = 0
    ),
    mid_ep = case_when(
      acm_ep == 1 & str_detect(primary_death_code, mi_rgx) ~ 1,
      .default = 0
    ),
    strkd_ep = case_when(
      acm_ep == 1 & str_detect(primary_death_code, sk_rgx) ~ 1,
      .default = 0
    ),
    kidd_ep = case_when(
      acm_ep == 1 & str_detect(primary_death_code, kd_rgx) ~ 1,
      .default = 0
    ),
    cancd_ep = case_when(
      acm_ep == 1 & str_detect(primary_death_code, cn_rgx) ~ 1,
      .default = 0
    ),
    neurod_ep = case_when(
      acm_ep == 1 & str_detect(primary_death_code, ne_rgx) ~ 1,
      .default = 0
    ),
    asthmd_ep = case_when(
      acm_ep == 1 & str_detect(primary_death_code, as_rgx) ~ 1,
      .default = 0
    ),
    dbd_ep = case_when(
      acm_ep == 1 & str_detect(primary_death_code, db_rgx) ~ 1,
      .default = 0
    ),
    covidd_ep = case_when(
      acm_ep == 1 & primary_death_code %in% covid_death ~ 1,
      .default = 0
    ),
    infd_ep = case_when(
      acm_ep == 1 & str_detect(primary_death_code, in_rgx) ~ 1,
      .default = 0
    ),
    injd_ep = case_when(
      acm_ep == 1 & str_detect(primary_death_code, ij_rgx) ~ 1,
      .default = 0
    ),
    aid_ep = case_when(
      acm_ep == 1 & str_detect(primary_death_code, ai_rgx) ~ 1,
      .default = 0
    ),
    acm_date = case_when(
      !is.na(death_date) & death_date >= discharge_date ~ death_date,
      !is.na(death_date) & death_date < discharge_date  ~ discharge_date,
      is.na(death_date) & !is.na(discharge_date) ~ max(death_date, na.rm = TRUE),
      .default = NA
    ),
    acm_tte = as.numeric(acm_date - discharge_date) + 1,
    across(
      ends_with("_ep"), \(x) case_when(
        x == 1 & acm_tte <= 365.25 ~ 1,
        x == 1 & acm_tte > 365.25 ~ 0,
        x == 0 ~ 0,
        .default = NA
      ), .names = "{.col}_1yr"
    ),
    acm_tte_1yr = case_when(
      acm_tte > 365.25 ~ 365.25,
      .default = acm_tte
    )
  ) |>
  select(-c(admission_date, discharge_date))


# Write data --------------------------------------------------------------

save(dt3, "deaths")
