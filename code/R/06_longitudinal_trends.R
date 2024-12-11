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

libs <- c("here", "lubridate", "tidyverse", "bit64", "cmprsk")

for (l in libs) library(l, character.only = TRUE)


# Define variables --------------------------------------------------------

# Pipeline version
ver <- "v2"

# Today's date
date <- tolower(format(Sys.Date(), "%d%b%Y"))


# Create new sub-directories ----------------------------------------------

dir.create(here::here("figs", "incidence_patterns"), showWarnings = FALSE)
dir.create(here::here("figs", "outcome_trends"), showWarnings = FALSE)


# Source functions --------------------------------------------------------

fns <- c(
  "load_data_locally", "rnd", "save", "get_counts",
  "get_temporal_trends_overall", "get_temporal_trends_by_subgroup"
)

for (f in fns) source(here::here("src", paste0(f, ".R")))


# Load data ---------------------------------------------------------------

# Analysis data
ad <- load_data_locally(.from = "output", .table = "analysis_data")


# Heart failure cases by month --------------------------------------------

# Counts for all heart failure
inc_all_hf <- ad |> get_counts()

# Counts for new heart failure
inc_new_hf <- ad |> filter(hhf == 0) |> get_counts()

# Counts for recurrent heart failure
inc_rec_hf <- ad |> filter(hhf == 1) |> get_counts()


# Code outcomes -----------------------------------------------------------

# Composite for all-cause mortality and all-cause emergency re-hospitalisation
ad2 <- ad |>
  mutate(
    acmh_ep = case_when(
      acm_ep == 1 | ach_emerg_ep == 1 ~ 1,
      .default = 0
    ),
    acmh_date = case_when(
      acmh_ep == 1 ~ pmin(acm_date, ach_emerg_date),
      .default = pmax(acm_date, ach_emerg_date)
    ),
    acmh_tte = as.numeric(acmh_date - discharge_date) + 1
  )


# Recode data for incidence rate analysis ---------------------------------

# Define time horizon for mortality outcomes
time_30d <- 30
time_1yr <- 365.25

rate <- ad2 |> 
  # Remove patients with death date before index admission, patients that
  # died during their very first hospitalisation for heart failure, and patients
  # with a history of heart failure
  filter(error == 0, hhf == 0, !(in_hosp_death == 1 & hhf == 0)) |>
  dplyr::select(
    subject_id, admission_date, hf_fct, age, sex, ethnic, imd_fct, hhf, egfr,
    acm_ep, cvd_ep, non_cvd_ep, hfd_ep, acm_tte, acmh_ep, acmh_tte, 
    ach_emerg_ep, ach_emerg_tte, hcv_pri_emerg_ep, hcv_pri_emerg_tte, 
    hncv_pri_emerg_ep, hncv_pri_emerg_tte, hhf_pri_emerg_ep, hhf_pri_emerg_tte,
    matches("6mo")
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
    # Add indicator for whether admission occurred during during a COVID-19 period
    covid_period = case_when(
      date_month == "2020-02-01" | date_month == "2020-03-01" |
      date_month == "2020-11-01" | date_month == "2020-12-01" |
      date_month == "2021-01-01" | date_month == "2021-02-01" ~ 1,
      .default = 0
    ),
    # Medications stuff
    across(matches("6mo_after"), \(x) if_else(x == 0, 0, as.integer(x / 1))),
    ACE_ARB_6mo_after = case_when(
      if_any(c(ACEI_6mo_after, ARB_6mo_after), \(x) x == 1) ~ 1,
      .default = 0
    ),
    RAS_6mo_after = case_when(
      if_any(c(ACEI_6mo_after, ARB_6mo_after, ARNI_6mo_after), \(x) x == 1) ~ 1,
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
    # Recode mortality outcomes
    across(
      matches("m_ep$|d_ep$"), \(x) case_when(
        x == 1 & acm_tte <= time_30d ~ 1,
        x == 1 & acm_tte > time_30d ~ 0,
        x == 0 ~ 0, 
        .default = NA
      ), .names = "{.col}_30d"
    ),
    across(
      matches("m_ep$|d_ep$"), \(x) case_when(
        x == 1 & acm_tte <= time_1yr ~ 1,
        x == 1 & acm_tte > time_1yr ~ 0,
        x == 0 ~ 0, 
        .default = NA
      ), .names = "{.col}_1yr"
    ),
    ach_emerg_ep_30d = case_when(
      ach_emerg_ep == 1 & ach_emerg_tte <= time_30d ~ 1,
      ach_emerg_ep == 1 & ach_emerg_tte > time_30d ~ 0,
      ach_emerg_ep == 0 ~ 0, 
      .default = NA
    ),
    ach_emerg_ep_1yr = case_when(
      ach_emerg_ep == 1 & ach_emerg_tte <= time_1yr ~ 1,
      ach_emerg_ep == 1 & ach_emerg_tte > time_1yr ~ 0,
      ach_emerg_ep == 0 ~ 0, 
      .default = NA
    ),
    hcv_pri_emerg_ep_30d = case_when(
      hcv_pri_emerg_ep == 1 & hcv_pri_emerg_tte <= time_30d ~ 1,
      hcv_pri_emerg_ep == 1 & hcv_pri_emerg_tte > time_30d ~ 0,
      hcv_pri_emerg_ep == 0 ~ 0,
      .default = NA
    ),
    hcv_pri_emerg_ep_1yr = case_when(
      hcv_pri_emerg_ep == 1 & hcv_pri_emerg_tte <= time_1yr ~ 1,
      hcv_pri_emerg_ep == 1 & hcv_pri_emerg_tte > time_1yr ~ 0,
      hcv_pri_emerg_ep == 0 ~ 0,
      .default = NA
    ),
    hncv_pri_emerg_ep_30d = case_when(
      hncv_pri_emerg_ep == 1 & hncv_pri_emerg_tte <= time_30d ~ 1,
      hncv_pri_emerg_ep == 1 & hncv_pri_emerg_tte > time_30d ~ 0,
      hncv_pri_emerg_ep == 0 ~ 0,
      .default = NA
    ),
    hncv_pri_emerg_ep_1yr = case_when(
      hncv_pri_emerg_ep == 1 & hncv_pri_emerg_tte <= time_1yr ~ 1,
      hncv_pri_emerg_ep == 1 & hncv_pri_emerg_tte > time_1yr ~ 0,
      hncv_pri_emerg_ep == 0 ~ 0,
      .default = NA
    ),
    hhf_pri_emerg_ep_30d = case_when(
      hhf_pri_emerg_ep == 1 & hhf_pri_emerg_tte <= time_30d ~ 1,
      hhf_pri_emerg_ep == 1 & hhf_pri_emerg_tte > time_30d ~ 0,
      hhf_pri_emerg_ep == 0 ~ 0,
      .default = NA
    ),
    hhf_pri_emerg_ep_1yr = case_when(
      hhf_pri_emerg_ep == 1 & hhf_pri_emerg_tte <= time_1yr ~ 1,
      hhf_pri_emerg_ep == 1 & hhf_pri_emerg_tte > time_1yr ~ 0,
      hhf_pri_emerg_ep == 0 ~ 0,
      .default = NA
    ),
    acmh_ep_30d = case_when(
      acmh_ep == 1 & acmh_tte <= time_30d ~ 1,
      acmh_ep == 1 & acmh_tte > time_30d ~ 0,
      acmh_ep == 0 ~ 0,
      .default = NA
    ),
    acmh_ep_1yr = case_when(
      acmh_ep == 1 & acmh_tte <= time_1yr ~ 1,
      acmh_ep == 1 & acmh_tte > time_1yr ~ 0,
      acmh_ep == 0 ~ 0,
      .default = NA
    ),
    across(
      ends_with("_tte"), \(x) case_when(
        x > time_30d ~ time_30d / 365.25,
        .default = x / 365.25
      ), .names = "{.col}_30d"
    ),
    across(
      ends_with("_tte"), \(x) case_when(
        x > time_1yr ~ time_1yr / 365.25,
        .default = x / 365.25
      ), .names = "{.col}_1yr"
    ),
    # Center age
    age_mean = mean(age), 
    age_c = age - age_mean,
    .keep = "unused"
  )

# Cut data for annual trends
rate_22 <- rate |> filter(!str_detect(date_month, "2023"))


# Mortality rates following heart failure hospitalisation (overall)--------

fit_quasi_poisson <- function(.data, 
                              .ep_var,
                              .tte_var,
                              .date_var,
                              .subgroup = "") {
  
  if (.subgroup == "") {
    fit <- 
      glm(
        as.formula(
          paste0(
            .ep_var, " ~ ", .date_var, "+ age_c + offset(log(", .tte_var, "))"
          )
        ),
        family = "quasipoisson", data = .data
      )
    message("Model completed.")
  } else if (.subgroup == "age_grp") {
    fit <-
      glm(
        as.formula(
          paste0(
            .ep_var, " ~ ", .date_var, "*", .subgroup, 
            " + offset(log(", .tte_var, "))"
          )
        ), family = "quasipoisson", data = .data
      )
    message("Model completed.")
  } else if (.subgroup != "age_grp") {
    fit <-
      glm(
        as.formula(
          paste0(
            .ep_var, " ~ ", .date_var, "*", .subgroup, 
            " + age_c + offset(log(", .tte_var, "))"
          )
        ), 
        family = "quasipoisson", data = .data
      )
    message("Model completed.")
  }
  
  return(fit)
}

# 30-day mortality
eps_30d <- tribble(
  ~ep, ~tte, ~name,
  "acm_ep_30d", "acm_tte_30d", "All-cause mortality",
  "cvd_ep_30d", "acm_tte_30d", "Cardiovascular death",
  "non_cvd_ep_30d", "acm_tte_30d", "Non-cardiovascular death",
  "hfd_ep_30d", "acm_tte_30d", "Heart failure death",
  "acmh_ep_30d", "acmh_tte_30d", "All-cause mortality or emergency hospitalisation",
  "ach_emerg_ep_30d", "ach_emerg_tte_30d", "All-cause emergency hospitalisation",
  "hcv_pri_emerg_ep_30d", "hcv_pri_emerg_tte_30d", "Cardiovascular hospitalisation",
  "hncv_pri_emerg_ep_30d", "hncv_pri_emerg_tte_30d", "Non-cardiovascular hospitalisation",
  "hhf_pri_emerg_ep_30d", "hhf_pri_emerg_tte_30d", "Heart failure hospitalisation"
)
mods_30d <- list()

for (e in 1:nrow(eps_30d)) {
  mod <- fit_quasi_poisson(
    .data = rate, .ep_var = eps_30d$ep[e], .tte_var = eps_30d$tte[e],
    .date_var = "date_month"
  )
  mod <- get_temporal_trends_overall(
    .data = rate, .object = mod, .ep_var = !!sym(eps_30d$ep[e]), 
    .tte_var = !!sym(eps_30d$tte[e]), .date_var = "date_month"
  ) |>
    mutate(ep = eps_30d$name[e])
  mods_30d[[e]] <- mod
}
mods_30d <- mods_30d |> bind_rows()

# Annual reduction in 30-day outcomes
annual_30d <- list()

for (e in 1:nrow(eps_30d)) {
  ann <- get_annual_reduction(
    rate_22, .ep_var = eps_30d$ep[e], .tte_var = eps_30d$tte[e], 
    .ep_name = eps_30d$name[e], .by = "hf_fct"
  )
  annual_30d[[e]] <- ann
}
annual_30d <- annual_30d |> bind_rows()

# 1 year mortality
eps_1yr <- tribble(
  ~ep, ~tte, ~name,
  "acm_ep_1yr", "acm_tte_1yr", "All-cause mortality",
  "cvd_ep_1yr", "acm_tte_1yr", "Cardiovascular death",
  "non_cvd_ep_1yr", "acm_tte_1yr", "Non-cardiovascular death",
  "hfd_ep_1yr", "acm_tte_1yr", "Heart failure death",
  "acmh_ep_1yr", "acmh_tte_1yr", "All-cause mortality or emergency hospitalisation",
  "ach_emerg_ep_1yr", "ach_emerg_tte_1yr", "All-cause emergency hospitalisation",
  "hcv_pri_emerg_ep_1yr", "hcv_pri_emerg_tte_1yr", "Cardiovascular hospitalisation",
  "hncv_pri_emerg_ep_1yr", "hncv_pri_emerg_tte_1yr", "Non-cardiovascular hospitalisation",
  "hhf_pri_emerg_ep_1yr", "hhf_pri_emerg_tte_1yr", "Heart failure hospitalisation"
)

# 1 year mortality by month
mods_1yr <- list()

for (e in 1:nrow(eps_1yr)) {
  mod <- fit_quasi_poisson(
    .data = rate, .ep_var = eps_1yr$ep[e], .tte_var = eps_1yr$tte[e],
    .date_var = "date_month"
  )
  mod <- get_temporal_trends_overall(
    .data = rate, .object = mod, .ep_var = !!sym(eps_1yr$tte[e]), 
    .tte_var = !!sym(eps_1yr$tte[e]), .date_var = "date_month"
  ) |>
    mutate(ep = eps_1yr$name[e])
  mods_1yr[[e]] <- mod
}
mods_1yr <- mods_1yr |> bind_rows()

# Annual reduction in 1-year outcomes
annual_1yr <- list()

for (e in 1:nrow(eps_1yr)) {
  ann <- get_annual_reduction(
    rate_22, .ep_var = eps_1yr$ep[e], .tte_var = eps_1yr$tte[e], 
    .ep_name = eps_1yr$name[e]
  )
  annual_1yr[[e]] <- ann
}
annual_1yr <- annual_1yr |> bind_rows()


# Annual reductions for subgroups -----------------------------------------

get_annual_reduction <- function(.data, .ep_var, .tte_var, .ep_name, .by = NULL) {
  if (missing(.by)) {
    res <- 
      glm(
        as.formula(
          paste0(
            .ep_var, " ~ ", "as.numeric(date_month) + age_c + covid_period + offset(log(", .tte_var, "))"
          )
        ),
        family = "quasipoisson", data = .data
      ) |>
      broom::tidy(exponentiate = TRUE, conf.int = TRUE) |>
      filter(str_detect(term, "date_month"))
  } else if (.by == "hf_fct") {
    ref <- .data |> filter(hf_fct == "hfref")
    pef <- .data |> filter(hf_fct == "hfpef")
    uns <- .data |> filter(hf_fct == "unspecified")
    fit_ref <- 
      glm(
        as.formula(
          paste0(
            .ep_var, " ~ ", "as.numeric(date_month) + age_c + covid_period + offset(log(", .tte_var, "))"
          )
        ),
        family = "quasipoisson", data = ref
      ) |>
      broom::tidy(exponentiate = TRUE, conf.int = TRUE) |>
      filter(str_detect(term, "date_month")) |>
      mutate(grp = "hfref")
    fit_pef <- 
      glm(
        as.formula(
          paste0(
            .ep_var, " ~ ", "as.numeric(date_month) + age_c + covid_period + offset(log(", .tte_var, "))"
          )
        ),
        family = "quasipoisson", data = pef
      ) |>
      broom::tidy(exponentiate = TRUE, conf.int = TRUE) |>
      filter(str_detect(term, "date_month")) |>
      mutate(grp = "hfpef")
    fit_uns <- 
      glm(
        as.formula(
          paste0(
            .ep_var, " ~ ", "as.numeric(date_month) + age_c + covid_period + offset(log(", .tte_var, "))"
          )
        ),
        family = "quasipoisson", data = uns
      ) |>
      broom::tidy(exponentiate = TRUE, conf.int = TRUE) |>
      filter(str_detect(term, "date_month")) |>
      mutate(grp = "unspecified")
    res <- bind_rows(fit_ref, fit_pef, fit_uns)
  }
  
  res2 <- res |>
    mutate(
      ep = .ep_name,
      # 1 year
      per_year = as.character(rnd((1 - estimate ^ 12) * -100, 2)),
      per_year_lci = as.character(rnd((1 - conf.low ^ 12) * -100, 2)),
      per_year_uci = as.character(rnd((1 - conf.high ^ 12) * -100, 2)),
      # 4 years
      # four_year = as.character(rnd((1 - estimate ^ 48) * 100, 2)),
      # four_year_lci = as.character(rnd((1 - conf.high ^ 48) * 100, 2)),
      # four_year_uci = as.character(rnd((1 - conf.low ^ 48) * 100, 2)),
      across(
        matches("per_year|four_year"), \(x) case_when(
          str_detect(x, "[.][0-9]{1}$") ~ paste0(x, "0"),
          .default = x
        )
      ),
      py = paste0(per_year, "% (95% CI, ", per_year_lci, " to ", per_year_uci, ")"),
      # fy = paste0(four_year, "% (95% CI, ", four_year_lci, " to ", four_year_uci, ")"),
    ) |>
    select(ep, py, p.value)
  
  if (!missing(.by)) {
    res2 <- res2 |> bind_cols(res |> select(grp))
  }
  
  return(res2)
}

rate_spe <- rate_22 |> filter(str_detect(hf_fct, "hfref|hfpef"))

# REF and PEF only
annual_30d_spe <- list()
for (e in 1:nrow(eps_30d)) {
  ann <- get_annual_reduction(
    rate_22, .ep_var = eps_30d$ep[e], .tte_var = eps_30d$tte[e], 
    .ep_name = eps_30d$name[e]
  )
  annual_30d_spe[[e]] <- ann
}
annual_30d_spe <- annual_30d_spe |> bind_rows()

annual_1yr_spe <- list()
for (e in 1:nrow(eps_1yr)) {
  ann <- get_annual_reduction(
    rate_22, .ep_var = eps_1yr$ep[e], .tte_var = eps_1yr$tte[e], 
    .ep_name = eps_1yr$name[e]
  )
  annual_1yr_spe[[e]] <- ann
}
annual_1yr_spe <- annual_1yr_spe |> bind_rows()


# Annual reduction in 30-day outcomes
annual_30d_hf_fct <- list()

for (e in 1:nrow(eps_30d)) {
  ann <- get_annual_reduction(
    rate_22, .ep_var = eps_30d$ep[e], .tte_var = eps_30d$tte[e], 
    .ep_name = eps_30d$name[e], .by = "hf_fct"
  )
  annual_30d_hf_fct[[e]] <- ann
}
annual_30d_hf_fct <- annual_30d_hf_fct |> bind_rows()


# Annual reduction in 1-year outcomes
annual_1yr_hf_fct <- list()

for (e in 1:nrow(eps_1yr)) {
  ann <- get_annual_reduction(
    rate_22, .ep_var = eps_1yr$ep[e], .tte_var = eps_1yr$tte[e], 
    .ep_name = eps_1yr$name[e], .by = "hf_fct"
  )
  annual_1yr_hf_fct[[e]] <- ann
}
annual_1yr_hf_fct <- annual_1yr_hf_fct |> bind_rows()


# Competing risks regression for revisions
model_spec <- reformulate(
  paste(c("date_month", "age_c", "covid_period"), collapse = "+")
)

# Code competing risk
rate_spe2 <- rate_spe |>
  mutate(
    date_month = as.numeric(date_month),
    cmprsk_30d = case_when(
      ach_emerg_ep_30d == 1 ~ 1,
      acm_ep_30d == 1 & ach_emerg_ep_30d == 0 ~ 2,
      .default = 0
    ),
    cmprsk_1yr = case_when(
      ach_emerg_ep_1yr == 1 ~ 1,
      acm_ep_1yr == 1 & ach_emerg_ep_1yr == 0 ~ 2,
      .default = 0
    )
  ) |>
  select(
    subject_id, cmprsk_30d, ach_emerg_tte_30d, cmprsk_1yr, ach_emerg_tte_1yr,
    acm_ep_30d, acm_tte_30d, acm_ep_1yr, acm_tte_1yr, hf_fct, date_month, age_c,
    covid_period
  )
ref <- rate_spe2 |> filter(str_detect(hf_fct, "ref"))
pef <- rate_spe2 |> filter(str_detect(hf_fct, "pef"))

cov_mat_ref <- model.matrix(model_spec, data = ref)[, -1] 
cov_mat_pef <- model.matrix(model_spec, data = pef)[, -1] 

# 30-day
crsk30_ref <- 
  crr(
    ref$ach_emerg_tte_30d, ref$cmprsk_30d, cov_mat_ref, failcode = 1, 
    cencode = 0
  )
crsk30_pef <- 
  crr(
    pef$ach_emerg_tte_30d, pef$cmprsk_30d, cov_mat_pef, failcode = 1, 
    cencode = 0
  )
acm30_ref <- 
  crr(
    ref$acm_tte_30d, ref$acm_ep_30d, cov_mat_ref, failcode = 1, 
    cencode = 0
  )
acm30_pef <- 
  crr(
    pef$acm_tte_30d, pef$acm_ep_30d, cov_mat_pef, failcode = 1, 
    cencode = 0
  )

# 1-year
crsk1_ref <- 
  crr(
    ref$ach_emerg_tte_1yr, ref$cmprsk_1yr, cov_mat_ref, failcode = 1, 
    cencode = 0
  )
crsk1_pef <- 
  crr(
    pef$ach_emerg_tte_1yr, pef$cmprsk_1yr, cov_mat_pef, failcode = 1, 
    cencode = 0
  )
acm1_ref <- 
  crr(
    ref$acm_tte_1yr, ref$acm_ep_1yr, cov_mat_ref, failcode = 1, 
    cencode = 0
  )
acm1_pef <- 
  crr(
    pef$acm_tte_1yr, pef$acm_ep_1yr, cov_mat_pef, failcode = 1, 
    cencode = 0
  )

ref_res_30d <- as_tibble(summary(crsk30_ref)$coef) |>
  slice(1) |>
  rename(estimate = `exp(coef)`, se = `se(coef)`, p.value = `p-value`) |>
  mutate(
    grp = "HFrEF",
    ep = "30-day emergency re-hospitalisation",
    conf.low = exp(coef - 1.96 * se),
    conf.high = exp(coef + 1.96 * se),
    per_year = as.character(rnd((1 - estimate ^ 12) * -100, 2)),
    per_year_lci = as.character(rnd((1 - conf.low ^ 12) * -100, 2)),
    per_year_uci = as.character(rnd((1 - conf.high ^ 12) * -100, 2)),
    across(
      matches("per_year|four_year"), \(x) case_when(
        str_detect(x, "[.][0-9]{1}$") ~ paste0(x, "0"),
        .default = x
      )
    ),
    py = paste0(per_year, "% (95% CI, ", per_year_lci, " to ", per_year_uci, ")")
  ) |>
  select(grp, ep, py, p.value)

pef_res_30d <- as_tibble(summary(crsk30_pef)$coef) |>
  slice(1) |>
  rename(estimate = `exp(coef)`, se = `se(coef)`, p.value = `p-value`) |>
  mutate(
    grp = "HFpEF",
    ep = "30-day emergency re-hospitalisation",
    conf.low = exp(coef - 1.96 * se),
    conf.high = exp(coef + 1.96 * se),
    per_year = as.character(rnd((1 - estimate ^ 12) * -100, 2)),
    per_year_lci = as.character(rnd((1 - conf.low ^ 12) * -100, 2)),
    per_year_uci = as.character(rnd((1 - conf.high ^ 12) * -100, 2)),
    across(
      matches("per_year|four_year"), \(x) case_when(
        str_detect(x, "[.][0-9]{1}$") ~ paste0(x, "0"),
        .default = x
      )
    ),
    py = paste0(per_year, "% (95% CI, ", per_year_lci, " to ", per_year_uci, ")")
  ) |>
  select(grp, ep, py, p.value)

ref_res_1yr <- as_tibble(summary(crsk1_ref)$coef) |>
  slice(1) |>
  rename(estimate = `exp(coef)`, se = `se(coef)`, p.value = `p-value`) |>
  mutate(
    grp = "HFrEF",
    ep = "One-year emergency re-hospitalisation",
    conf.low = exp(coef - 1.96 * se),
    conf.high = exp(coef + 1.96 * se),
    per_year = as.character(rnd((1 - estimate ^ 12) * -100, 2)),
    per_year_lci = as.character(rnd((1 - conf.low ^ 12) * -100, 2)),
    per_year_uci = as.character(rnd((1 - conf.high ^ 12) * -100, 2)),
    across(
      matches("per_year|four_year"), \(x) case_when(
        str_detect(x, "[.][0-9]{1}$") ~ paste0(x, "0"),
        .default = x
      )
    ),
    py = paste0(per_year, "% (95% CI, ", per_year_lci, " to ", per_year_uci, ")")
  ) |>
  select(grp, ep, py, p.value)

pef_res_1yr <- as_tibble(summary(crsk1_pef)$coef) |>
  slice(1) |>
  rename(estimate = `exp(coef)`, se = `se(coef)`, p.value = `p-value`) |>
  mutate(
    grp = "HFpEF",
    ep = "One-year emergency re-hospitalisation",
    conf.low = exp(coef - 1.96 * se),
    conf.high = exp(coef + 1.96 * se),
    per_year = as.character(rnd((1 - estimate ^ 12) * -100, 2)),
    per_year_lci = as.character(rnd((1 - conf.low ^ 12) * -100, 2)),
    per_year_uci = as.character(rnd((1 - conf.high ^ 12) * -100, 2)),
    across(
      matches("per_year|four_year"), \(x) case_when(
        str_detect(x, "[.][0-9]{1}$") ~ paste0(x, "0"),
        .default = x
      )
    ),
    py = paste0(per_year, "% (95% CI, ", per_year_lci, " to ", per_year_uci, ")")
  ) |>
  select(grp, ep, py, p.value)

bind_rows(ref_res_30d, pef_res_30d, ref_res_1yr, pef_res_1yr) |>
  rename(
    `HF type` = grp, Outcome = ep, `Annual Trend in Incidence` = py,
    `P-value` = p.value
  )


# Mortality rates following heart failure hospitalisation (subgroups)------

# 30 day endpoints
eps_30d <- tribble(
  ~ep, ~tte, ~name,
  "acm_ep_30d", "acm_tte_30d", "All-cause mortality",
  "cvd_ep_30d", "acm_tte_30d", "Cardiovascular death",
  "non_cvd_ep_30d", "acm_tte_30d", "Non-cardiovascular death",
  "hfd_ep_30d", "acm_tte_30d", "Heart failure death",
  "acmh_ep_30d", "acmh_tte_30d", "All-cause mortality or emergency hospitalisation",
  "ach_emerg_ep_30d", "ach_emerg_tte_30d", "All-cause emergency hospitalisation",
  "hcv_pri_emerg_ep_30d", "hcv_pri_emerg_tte_30d", "Cardiovascular hospitalisation",
  "hncv_pri_emerg_ep_30d", "hncv_pri_emerg_tte_30d", "Non-cardiovascular hospitalisation",
  "hhf_pri_emerg_ep_30d", "hhf_pri_emerg_tte_30d", "Heart failure hospitalisation"
)

# 1 year endpoints
eps_1yr <- tribble(
  ~ep, ~tte, ~name,
  "acm_ep_1yr", "acm_tte_1yr", "All-cause mortality",
  "cvd_ep_1yr", "acm_tte_1yr", "Cardiovascular death",
  "non_cvd_ep_1yr", "acm_tte_1yr", "Non-cardiovascular death",
  "hfd_ep_1yr", "acm_tte_1yr", "Heart failure death",
  "acmh_ep_1yr", "acmh_tte_1yr", "All-cause mortality or emergency hospitalisation",
  "ach_emerg_ep_1yr", "ach_emerg_tte_1yr", "All-cause emergency hospitalisation",
  "hcv_pri_emerg_ep_1yr", "hcv_pri_emerg_tte_1yr", "Cardiovascular hospitalisation",
  "hncv_pri_emerg_ep_1yr", "hncv_pri_emerg_tte_1yr", "Non-cardiovascular hospitalisation",
  "hhf_pri_emerg_ep_1yr", "hhf_pri_emerg_tte_1yr", "Heart failure hospitalisation"
)

grps <- tibble(
  subs = c("hf_fct", "age_grp", "sex", "ethnic", "imd_fct", "egfr_grp")
)

join_30d <- cross_join(eps_30d, grps) |>
  group_by(row_number()) |> 
  group_map(~as.list(.x))
join_1yr <- cross_join(eps_1yr, grps) |>
  group_by(row_number()) |> 
  group_map(~as.list(.x))

mods_sub_30d <- list()
for (j in 1:length(join_30d)) {
  mod_sub_30d <- fit_quasi_poisson(
    .data = rate, .ep_var = join_30d[[j]]$ep, .tte_var = join_30d[[j]]$tte,
    .subgroup = join_30d[[j]]$subs, .date_var = "date_month"
  )
  mods_sub_30d[[j]] <- mod_sub_30d
  names(mods_sub_30d)[[j]] <- paste0(join_30d[[j]]$ep, "_", join_30d[[j]]$subs)
}

mods_sub_1yr <- list()
for (j in 1:length(join_1yr)) {
  mod_sub_1yr <- fit_quasi_poisson(
    .data = rate, .ep_var = join_1yr[[j]]$ep, .tte_var = join_1yr[[j]]$tte,
    .subgroup = join_1yr[[j]]$subs, .date_var = "date_month"
  )
  mods_sub_1yr[[j]] <- mod_sub_1yr
  names(mods_sub_1yr)[[j]] <- paste0(join_1yr[[j]]$ep, "_", join_1yr[[j]]$subs)
}

grps_vec <- as.vector(grps$subs)
# Get lists for each subgroup
for (g in grps_vec) {
  # Create a variable name for the new list
  new_list_name_30d <- paste("mods", g, "30d", sep = "_")
  new_list_name_1yr <- paste("mods", g, "1yr", sep = "_")
  
  # Find elements that match the pattern
  matched_elements_30d <- grepl(paste0(g, "$"), names(mods_sub_30d))
  matched_elements_1yr <- grepl(paste0(g, "$"), names(mods_sub_1yr))
  
  # Subset the original list based on matched elements
  new_list_30d <- mods_sub_30d[matched_elements_30d]
  new_list_1yr <- mods_sub_1yr[matched_elements_1yr]
  
  # Dynamically create a new list variable with the specified name
  assign(new_list_name_30d, new_list_30d)
  assign(new_list_name_1yr, new_list_1yr)
}

# Get temporal trends for each subgroup
mods_hf_fct_30d_2 <- mods_hf_fct_30d |>
  map(
    \(x) get_temporal_trends_by_subgroup(
      x, .non_baseline_regex = c("hfpef", "unspecified"), 
      .groups = c("hfref", "hfpef", "unspecified"), .date_var = "date_month"
    )
  ) |>
  map2(eps_30d$name, \(x, y) mutate(x, name = y)) |>
  bind_rows()

mods_hf_fct_1yr_2 <- mods_hf_fct_1yr |>
  map(
    \(x) get_temporal_trends_by_subgroup(
      x, .non_baseline_regex = c("hfpef", "unspecified"), 
      .groups = c("hfref", "hfpef", "unspecified"), .date_var = "date_month"
    )
  ) |>
  map2(eps_1yr$name, \(x, y) mutate(x, name = y)) |>
  bind_rows()

mods_age_grp_30d_2 <- mods_age_grp_30d |>
  map(
    \(x) get_temporal_trends_by_subgroup(
      x, .non_baseline_regex = c("70 to 79", "80 to 89", "90 or more"), 
      .groups = c("Less than 70", "70 to 79", "80 to 89", "90 or more"), 
      .age_centered = FALSE, .date_var = "date_month"
    )
  ) |>
  map2(eps_30d$name, \(x, y) mutate(x, name = y)) |>
  bind_rows()

mods_age_grp_1yr_2 <- mods_age_grp_1yr |>
  map(
    \(x) get_temporal_trends_by_subgroup(
      x, .non_baseline_regex = c("70 to 79", "80 to 89", "90 or more"), 
      .groups = c("Less than 70", "70 to 79", "80 to 89", "90 or more"), 
      .age_centered = FALSE, .date_var = "date_month"
    )
  ) |>
  map2(eps_1yr$name, \(x, y) mutate(x, name = y)) |>
  bind_rows()

mods_sex_30d_2 <- mods_sex_30d |>
  map(
    \(x) get_temporal_trends_by_subgroup(
      x, .non_baseline_regex = c("female"), .groups = c("male", "female"),
      .date_var = "date_month"
    )
  ) |>
  map2(eps_30d$name, \(x, y) mutate(x, name = y)) |>
  bind_rows()

mods_sex_1yr_2 <- mods_sex_1yr |>
  map(
    \(x) get_temporal_trends_by_subgroup(
      x, .non_baseline_regex = c("female"), .groups = c("male", "female"),
      .date_var = "date_month"
    )
  ) |>
  map2(eps_1yr$name, \(x, y) mutate(x, name = y)) |>
  bind_rows()

mods_ethnic_30d_2 <- mods_ethnic_30d |>
  map(
    \(x) get_temporal_trends_by_subgroup(
      x, .non_baseline_regex = c("nonwhite", "missing"), 
      .groups = c("white", "nonwhite", "missing"), .date_var = "date_month"
    )
  ) |>
  map2(eps_30d$name, \(x, y) mutate(x, name = y)) |>
  bind_rows()

mods_ethnic_1yr_2 <- mods_ethnic_1yr |>
  map(
    \(x) get_temporal_trends_by_subgroup(
      x, .non_baseline_regex = c("nonwhite", "missing"), 
      .groups = c("white", "nonwhite", "missing"), .date_var = "date_month"
    )
  ) |>
  map2(eps_1yr$name, \(x, y) mutate(x, name = y)) |>
  bind_rows()

mods_imd_fct_30d_2 <- mods_imd_fct_30d |>
  map(
    \(x) get_temporal_trends_by_subgroup(
      x, 
      .non_baseline_regex = c("Two", "Three", "Four", "Privileged", "Missing"), 
      .groups = c("Deprived", "Two", "Three", "Four", "Privileged", "Missing"),
      .date_var = "date_month"
    )
  ) |>
  map2(eps_30d$name, \(x, y) mutate(x, name = y)) |>
  bind_rows()

mods_imd_fct_1yr_2 <- mods_imd_fct_1yr |>
  map(
    \(x) get_temporal_trends_by_subgroup(
      x, 
      .non_baseline_regex = c("Two", "Three", "Four", "Privileged", "Missing"), 
      .groups = c("Deprived", "Two", "Three", "Four", "Privileged", "Missing"),
      .date_var = "date_month"
    )
  ) |>
  map2(eps_1yr$name, \(x, y) mutate(x, name = y)) |>
  bind_rows()

mods_egfr_grp_30d_2 <- mods_egfr_grp_30d |>
  map(
    \(x) get_temporal_trends_by_subgroup(
      x, .non_baseline_regex = c("30 to less than 60", "Less than 30"), 
      .groups = c("60 or more", "30 to less than 60", "Less than 30"),
      .date_var = "date_month"
    )
  ) |>
  map2(eps_30d$name, \(x, y) mutate(x, name = y)) |>
  bind_rows()

mods_egfr_grp_1yr_2 <- mods_egfr_grp_1yr |>
  map(
    \(x) get_temporal_trends_by_subgroup(
      x, .non_baseline_regex = c("30 to less than 60", "Less than 30"), 
      .groups = c("60 or more", "30 to less than 60", "Less than 30"), 
      .date_var = "date_month"
    )
  ) |>
  map2(eps_1yr$name, \(x, y) mutate(x, name = y)) |>
  bind_rows()


# Separate results --------------------------------------------------------

separate_results <- function(.data, .new_name) {
  dth <- .data |> filter(str_detect(name, "death|mortality$"))
  hsp <- .data |> filter(str_detect(name, "hosp"))
  
  assign(paste0(.new_name, "_dth"), dth, envir = .GlobalEnv)
  assign(paste0(.new_name, "_hsp"), hsp, envir = .GlobalEnv)
}

models_sub <- tribble(
  ~df, ~new,
  mods_hf_fct_30d_2, "hf_fct_30d",
  mods_hf_fct_1yr_2, "hf_fct_1yr",
  mods_age_grp_30d_2, "age_grp_30d",
  mods_age_grp_1yr_2, "age_grp_1yr",
  mods_sex_30d_2, "sex_30d",
  mods_sex_1yr_2, "sex_1yr",
  mods_ethnic_30d_2, "ethnic_30d",
  mods_ethnic_1yr_2, "ethnic_1yr",
  mods_imd_fct_30d_2, "imd_fct_30d",
  mods_imd_fct_1yr_2, "imd_fct_1yr",
  mods_egfr_grp_30d_2, "egfr_30d",
  mods_egfr_grp_1yr_2, "egfr_1yr"
)

for (m in 1:nrow(models_sub)) {
  separate_results(models_sub$df[[m]], models_sub$new[m])
}


# Write data --------------------------------------------------------------

# Remove existing files in the output directory
file.remove(list.files(here::here("figs", "incidence_patterns"), full.names = TRUE))
file.remove(list.files(here::here("figs", "outcome_trends"), full.names = TRUE))

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
inc <- lst(inc_all_hf, inc_new_hf, inc_rec_hf)

for (t in seq_along(inc)) {
  export_table(inc[[t]], "incidence_patterns", names(inc)[t])
}

# Outcome trends
combined_list <- lst(
  mods_30d, mods_1yr, hf_fct_30d_dth, hf_fct_30d_hsp, hf_fct_1yr_dth, 
  hf_fct_1yr_hsp, age_grp_30d_dth, age_grp_30d_hsp, age_grp_1yr_dth, 
  age_grp_1yr_hsp, sex_30d_dth, sex_30d_hsp, sex_1yr_dth, sex_1yr_hsp, 
  ethnic_30d_dth, ethnic_30d_hsp, ethnic_1yr_dth, ethnic_1yr_hsp,
  imd_fct_30d_dth, imd_fct_30d_hsp, imd_fct_1yr_dth, imd_fct_1yr_hsp,
  egfr_30d_dth, egfr_30d_hsp, egfr_1yr_dth, egfr_1yr_hsp
)

for (t in seq_along(combined_list)) {
  export_table(combined_list[[t]], "outcome_trends", names(combined_list)[t])
}

