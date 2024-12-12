#*******************************************************************************
#
# Project: CCU005_04
# Title:   Trends and impact of multiple long-term conditions in heart failure
# Date:    19-Mar-2024
# Author:  Robert Fletcher
# Purpose: Describe baseline characteristics of patients
#
#*******************************************************************************


# Notes -------------------------------------------------------------------

# Open the `nicor` RStudio project to run this code


# Load libraries ----------------------------------------------------------

libs <- c("here", "lubridate", "tidyverse")
for (l in libs) library(l, character.only = TRUE)


# Define variables --------------------------------------------------------

# Today's date
date <- tolower(format(Sys.Date(), "%d%b%Y"))

# Vectors for categories of `.by` variables 
hf <- c("HFpEF", "HFrEF", "Unknown")
tp <- c("1", "2", "3", "4")
cm <- c("≥5", "0", "1", "2", "3", "4")
no <- c("0", "1")
dt <- c("0", "1")
nv <- c("0", "1")


# Create new sub-directory ------------------------------------------------

dir.create(here::here("figs", "baseline_table"), showWarnings = FALSE)


# Source functions --------------------------------------------------------

fns <- c(
  "load_data_locally", "summarise_continuous", "summarise_categorical",
  "define_characteristic_names", "describe_cohort", "describe_comorbidities"
)
for (f in fns) source(here::here("src", paste0(f, ".R")))


# Define functions --------------------------------------------------------

export_table <- function(.table, .dir, .name) {
  files <- length(list.files(here::here("figs", .dir)))
  if (files < 9) {
    num <- paste0("0", files + 1, "_")
  } else if (files >= 9) {
    num <- paste0(files + 1, "_")
  }
  write_csv(
    .table, here::here("figs", .dir, paste0(num, .name, ".csv"))
  )
}


# Load data ---------------------------------------------------------------

# Analysis data
ad <- load_data_locally(.from = "output", .table = "analysis_data")


# Define missing data variables -------------------------------------------

ad2 <- ad |>
  filter(
    admission_date < ymd("2023-01-01"),
    error == 0, in_hosp_death != 1, !is.na(site)
  ) |> 
  mutate(
    index_date = admission_date,
    new_hf = if_else(hhf == 1, 0, 1),
    tar = acm_tte / 365.25,
    period = case_when(
      index_date < ymd("2020-01-01") ~ 1,
      index_date >= ymd("2020-03-11") & index_date < ymd("2021-01-01") ~ 2,
      index_date >= ymd("2021-01-01") & index_date < ymd("2022-01-01") ~ 3,
      index_date >= ymd("2022-01-01") & index_date < ymd("2023-01-01") ~ 4,
      index_date >= ymd("2023-01-01") & index_date < ymd("2024-01-01") ~ 5
    ),
    nov19 = if_else(admission_date < ymd("2019-11-01"), 1, 0),
    across(
      c(bmi, sbp, egfr, k, bnp, nt_pro_bnp),
      \(x) if_else(is.na(x), 1, 0), .names = "{.col}_na"
    ),
    # Medications
    across(matches("6mo_after"), \(x) if_else(x == 0, 0, 1)),
    RAS_6mo_after = case_when(
      if_any(c(ACEI_6mo_after, ARB_6mo_after, ARNI_6mo_after), \(x) x == 1) ~ 1,
      .default = 0
    ),
    comorb_fct = fct_recode(comorb_fct, "zero" = "0"),
    ethnic = fct_recode(ethnic, "missing ethnic" = "missing"),
    imd_fct = fct_recode(imd_fct, "missing imd" = "Missing"),
    smok_fct = fct_recode(smok_fct, "missing smok" = "missing"),
    nyha_fct = fct_recode(nyha_fct, "missing nyha" = "Missing"),
    new_hf = if_else(hhf == 1, 0, 1),
    prior_hosp = if_else(is.na(prior_hosp), 0, as.integer(prior_hosp / 1)),
    new_hf_dth = if_else(new_hf == 1 & in_hosp_death == 1, 1, 0)
  )

# Filter for those for whom index date is new-onset heart failure
ad2_newhf <- ad2 |>
  filter(new_hf == 1)

# Filter for those for whom index date is chronic heart failure
ad2_chronichf <- ad2 |>
  filter(new_hf == 0)

# Filter for those admitted from November 2019 onwards
ad2_nov19 <- ad2 |>
  filter(admission_date >= ymd("2019-11-01"))

# Filter for those for whom index date is new-onset heart failure from November 
# 2019 onwards 
ad2_nov19_newhf <- ad2_nov19 |>
  filter(new_hf == 1)


# Produce tables by heart failure subclass --------------------------------

desc <-
  tribble(
    ~name_bl,                                   ~name_comorb,                             ~data,           ~by,          ~cat,
    # "cohort_characteristics",                   "cohort_comorbidities",                   ad2,             "hf_subtype", hf,
    # "cohort_newhf_characteristics",             "cohort_newhf_comorbidities",             ad2_newhf,       "hf_subtype", hf,
    # "cohort_chronichf_characteristics",         "cohort_chronichf_comorbidities",         ad2_chronichf,   "hf_subtype", hf,
    # "cohort_nov19_characteristics",             "cohort_nov19_comorbidities",             ad2_nov19,       "hf_subtype", hf,
    # "cohort_nov19_newhf_characteristics",       "cohort_nov19_newhf_comorbidities",       ad2_nov19_newhf, "hf_subtype", hf,
    # "period_characteristics",                   "period_comorbidities",                   ad2,             "period",     tp,
    # "period_newhf_characteristics",             "period_newhf_comorbidities",             ad2_newhf,       "period",     tp,
    # "period_nov19_characteristics",             "period_nov19_comorbidities",             ad2_nov19,       "period",     tp,
    # "period_nov19_newhf_characteristics",       "period_nov19_newhf_comorbidities",       ad2_nov19_newhf, "period",     tp, 
    # "comorbidity_characteristics",              "comorbidity_comorbidities",              ad2,             "comorb_fct", cm,
    # "comorbidity_newhf_characteristics",        "comorbidity_newhf_comorbidities",        ad2_newhf,       "comorb_fct", cm,
    # "comorbidity_nov19_characteristics",        "comorbidity_nov19_comorbidities",        ad2_nov19,       "comorb_fct", cm,
    # "comorbidity_nov19_newhf_characteristics",  "comorbidity_nov19_newhf_comorbidities",  ad2_nov19_newhf, "comorb_fct", cm, 
    # "new_hf_death_characteristics",             "new_hf_death_comorbidities",             ad2,             "new_hf_dth", dt,
    # "new_hf_death_newhf_characteristics",       "new_hf_death_newhf_comorbidities",       ad2_newhf,       "new_hf_dth", dt,
    # "new_hf_death_nov19_characteristics",       "new_hf_death_nov19_comorbidities",       ad2_nov19,       "new_hf_dth", dt,
    # "new_hf_death_nov19_newhf_characteristics", "new_hf_death_nov19_newhf_comorbidities", ad2_nov19_newhf, "new_hf_dth", dt, 
    # "newhf_strat_characteristics",              "newhf_strat_comorbidities",              ad2,             "new_hf",     no,
    "newhf_nov19_strat_characteristics",        "newhf_nov19_strat_comorbidities",        ad2,             "new_hf",     no,
    "revision_nov19_yn_characteristics",        "revision_nov19_yn_comorbidities",        ad2,             "nov19",      nv
  ) |>
  group_by(row_number()) |>
  group_map( ~ as.list(.x))

res_bl <- list()
res_comorb <- list()
for (d in 1:length(desc)) {
  # Baseline
  tbl <- 
    describe_cohort(
      .data = desc[[d]]$data[[1]], .by = desc[[d]]$by, .grp = desc[[d]]$cat[[1]]
    )
  res_bl[[d]] <- tbl
  names(res_bl)[[d]] <- desc[[d]]$name_bl
  
  # Comorbidities
  tbl2 <- 
    describe_comorbidities(
      .data = desc[[d]]$data[[1]], .by = desc[[d]]$by, .grp = desc[[d]]$cat[[1]]
    )
  res_comorb[[d]] <- tbl2
  names(res_comorb)[[d]] <- desc[[d]]$name_comorb
}


# Export figures ----------------------------------------------------------

# Remove existing files in the output directory
file.remove(list.files(here::here("figs", "baseline_table"), full.names = TRUE))

export_table <- function(.table, .dir, .name) {
  files <- length(list.files(here::here("figs", .dir)))
  if (files < 9) {
    num <- paste0("0", files + 1, "_")
  } else if (files >= 9) {
    num <- paste0(files + 1, "_")
  }
  write_csv(
    .table, here::here("figs", .dir, paste0(num, .name, ".csv"))
  )
}

combined <- append(res_bl, res_comorb)

for (c in seq_along(combined)) {
  export_table(combined[[c]], "baseline_table", names(combined)[c])
}
