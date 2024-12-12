#*******************************************************************************
#
# Project: CCU005_04
# Title:   Trends and impact of multiple long-term conditions in heart failure
# Date:    01-May-2024
# Author:  Robert Fletcher
# Purpose: Prepare data for multiple imputation (i.e. biomarkers outside of the
#          window initially examined)
#
#*******************************************************************************


# Notes -------------------------------------------------------------------

# Open the `nicor` RStudio project to run this code


# Load libraries ----------------------------------------------------------

libs <- c("here", "lubridate", "mice", "tidyverse", "bit64")
for (l in libs) library(l, character.only = TRUE)


# Source functions --------------------------------------------------------

fns <- c("load_data_locally", "prepare_previous_values")

for (f in fns) source(here::here("src", paste0(f, ".R")))


# Define variables --------------------------------------------------------

# Repeat measures data
rm_files <- grep(
  "_repeat_measures", list.files(here::here("output"), full.names = TRUE),
  value = TRUE
)
rm_data <- grep(
  "_repeat_measures", list.files(here::here("output"), full.names = FALSE),
  value = TRUE
)


# Load data ---------------------------------------------------------------

# Analysis data
ad <- load_data_locally(.from = "output", .table = "analysis_data") 

purrr::walk2(
  rm_files, rm_data |> str_replace_all("_repeat_measures[.]rds$", ""),
  ~ assign(.y, read_rds(.x), envir = .GlobalEnv)
)


# Prepare body-mass index data from weight data ---------------------------

bmi_wght <- weight |> 
  filter(!is.na(height_bl)) |>
  mutate(
    across(c(height_bl, weight_bl), \(x) as.numeric(x)),
    bmi = weight_kg / (height_bl / 100)^2
  ) |>
  select(subject_id, admission_date, discharge_date, date, bmi)
bmi2 <- bind_rows(bmi, bmi_wght)


# Prepare previous weight, BMI, SBP, and eGFR values ----------------------

mean_weight <- prepare_previous_values(weight, weight_kg, mean_weight)
mean_bmi <- prepare_previous_values(bmi2, bmi, mean_bmi)
mean_sbp <- prepare_previous_values(sbp, sbp, mean_sbp)
mean_gfr <- prepare_previous_values(gfr, egfr, mean_egfr)


# Prepare previous smoking and NYHA ---------------------------------------

smoking_former <- smoking |>
  filter(date <= admission_date, is.na(smoking_bl) & !is.na(smoking)) |>
  mutate(diff = as.numeric(admission_date - date) / 365.25) |>
  group_by(subject_id) |>
  arrange(diff, smoking) |>
  slice(1) |>
  ungroup() |>
  select(subject_id, smoking_former = smoking)

nyha_former <- nyha |>
  filter(date <= admission_date, is.na(nyha_bl) & !is.na(nyha  )) |>
  mutate(diff = as.numeric(admission_date - date) / 365.25) |>
  group_by(subject_id) |>
  arrange(diff, desc(nyha)) |>
  slice(1) |>
  ungroup() |>
  select(subject_id, nyha_former = nyha)


# Create imputation-ready data --------------------------------------------

select <- ad |> 
  filter(
    admission_date >= ymd("2019-11-01"), admission_date < ymd("2023-01-01"),
    error == 0, in_hosp_death != 1, !is.na(site)
  ) |>
  mutate(
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
    bmi_fct = case_when(
      bmi < 18.5 ~ 1, 
      bmi >= 18.5 & bmi < 25 ~ 2,
      bmi >= 25 & bmi < 30 ~ 3,
      bmi >= 30 ~ 4,
      .default = NA
    ),
    bmi_fct = factor(
      bmi_fct, levels = c(1:4), 
      labels = c("<18.5", "18.5 to <25", "25 to <30", ">=30")
    ),
    sbp_fct = case_when(
      sbp < 120 ~ 1,
      sbp >= 120 & sbp < 130 ~ 2,
      sbp >= 130 & sbp < 140 ~ 3,
      sbp >= 140 & sbp < 150 ~ 4,
      sbp >= 150 ~ 5,
      .default = NA
    ),
    # Prior hospitalisations
    across(matches("prior_hosp|prior_gp"), \(x) if_else(x == 0, 0, as.numeric(x / 1))),
    sbp_fct = factor(
      sbp_fct, levels = c(1:5),
      labels = c("<120", "120 to 129", "130 to 139", "140 to 149", ">=150")
    ),
    ach_emerg_ep_1yr = case_when(
      ach_emerg_ep == 1 & ach_emerg_tte <= 365.25 ~ 1,
      ach_emerg_ep == 1 & ach_emerg_tte > 365.25 ~ 0,
      ach_emerg_ep == 0 ~ 0, 
      .default = NA
    ),
    hcv_pri_emerg_ep_1yr = case_when(
      hcv_pri_emerg_ep == 1 & hcv_pri_emerg_tte <= 365.25 ~ 1,
      hcv_pri_emerg_ep == 1 & hcv_pri_emerg_tte > 365.25 ~ 0,
      hcv_pri_emerg_ep == 0 ~ 0,
      .default = NA
    ),
    hncv_pri_emerg_ep_1yr = case_when(
      hncv_pri_emerg_ep == 1 & hncv_pri_emerg_tte <= 365.25 ~ 1,
      hncv_pri_emerg_ep == 1 & hncv_pri_emerg_tte > 365.25 ~ 0,
      hncv_pri_emerg_ep == 0 ~ 0,
      .default = NA
    ),
    hhf_pri_emerg_ep_1yr = case_when(
      hhf_pri_emerg_ep == 1 & hhf_pri_emerg_tte <= 365.25 ~ 1,
      hhf_pri_emerg_ep == 1 & hhf_pri_emerg_tte > 365.25 ~ 0,
      hhf_pri_emerg_ep == 0 ~ 0,
      .default = NA
    ),
    hcv19_pri_emerg_ep_1yr = case_when(
      hcv19_pri_emerg_ep == 1 & hcv19_pri_emerg_tte <= 365.25 ~ 1,
      hcv19_pri_emerg_ep == 1 & hcv19_pri_emerg_tte > 365.25 ~ 0,
      hcv19_pri_emerg_ep == 0 ~ 0,
      .default = NA
    ),
    across(
      ends_with("emerg_tte"), \(x) case_when(
        x > 365.25 ~ 365.25,
        .default = x
      ), .names = "{.col}_1yr"
    )
  ) |>
  select(
    subject_id, spell_id, admission_date, discharge_date, hf_subtype, hf_fct,
    age, age_grp, sex, ethnic, imd_fct, height_cm, weight_kg, bmi, sbp, egfr, 
    k, nyha, smoking, hhf, hcv19, hdb, hckd, hcanc, hdem, hdep, hobes, host,
    hthy, hasthm, hcopd, hhtn, hihd, hpad, hstk, hanae, haf, hlivd, hvalvhd, 
    hautoimmune, haf, hdicard, site, provider_1, hcv19_hosp, adm_type, 
    comorb_num, comorb_fct, error, prior_hosp_1yr,
    care_home, matches("ep_1yr$|tte_1yr$")
  )

ad_mi <-
  purrr::reduce(
    list(
      select, mean_weight, mean_bmi, mean_sbp, mean_gfr, smoking_former, 
      nyha_former
    ),
    left_join, by = join_by(subject_id)
  ) |>
  mutate(across(c(nyha, nyha_former), factor)) |>
  relocate(mean_weight, .after = "weight_kg") |>
  relocate(mean_bmi, .after = "bmi") |>
  relocate(mean_sbp, .after = "sbp") |>
  relocate(mean_egfr, .after = "egfr") |>
  relocate(smoking_former, .after = "smoking") |>
  relocate(nyha_former, .after = "nyha") |>
  mutate(
    smoking = factor(smoking),
    smoking_former = factor(smoking_former),
    # Recode infinite values for mean eGFR auxiliary variable
    mean_egfr = if_else(mean_egfr > 1000, NA, mean_egfr)
  )


# Compute Nelson-Aalen estimators -----------------------------------------

ad_mi2 <- ad_mi |>
  mutate(
    across(
      matches("acm_ep_1yr$|d_ep_1yr$"), 
      \(x) if_else(error == 0, mice::nelsonaalen(ad_mi, acm_tte_1yr, x), NA),
      .names = "{.col}_na"
    ),
    ach_ep_na = mice::nelsonaalen(ad_mi, ach_emerg_tte_1yr, ach_emerg_ep_1yr),
    hhf_pri_ep_na = mice::nelsonaalen(ad_mi, hhf_pri_emerg_tte_1yr, hhf_pri_emerg_ep_1yr),
    hcv_pri_ep_na = mice::nelsonaalen(ad_mi, hcv_pri_emerg_tte_1yr, hcv_pri_emerg_ep_1yr),
    hncv_pri_ep_na = mice::nelsonaalen(ad_mi, hncv_pri_emerg_tte_1yr, hncv_pri_emerg_ep_1yr),
    hcv19_pri_ep_na = mice::nelsonaalen(ad_mi, hcv19_pri_emerg_tte_1yr, hcv19_pri_emerg_ep_1yr),
  )


# Conduct multiple imputation ---------------------------------------------

# Get predictor matrix
ini <- mice(ad_mi2, maxit=0, print=F)
pred <- ini$pred

# Modify predictor matrix
pred[, 
     c(
       "age_grp", "error", "acm_ep_1yr", "cvd_ep_1yr", "non_cvd_ep_1yr", "hfd_ep_1yr", 
       "mid_ep_1yr", "strkd_ep_1yr", "kidd_ep_1yr", "cancd_ep_1yr", "neurod_ep_1yr", 
       "dbd_ep_1yr", "covidd_ep_1yr", "infd_ep_1yr", "injd_ep_1yr", "aid_ep_1yr", 
       "acm_tte_1yr", "ach_emerg_ep_1yr", "ach_emerg_tte_1yr", "hhf_pri_emerg_ep_1yr", 
       "hhf_pri_emerg_tte_1yr", "hcv_pri_emerg_ep_1yr", "hcv_pri_emerg_tte_1yr",
       "hncv_pri_emerg_ep_1yr", "hncv_pri_emerg_tte_1yr", "hcv19_pri_emerg_ep_1yr",
       "hcv19_pri_emerg_tte_1yr"
     )
] <- 0

pmm <- c(
  "height_cm", "weight_kg", "mean_weight", "mean_bmi", "mean_sbp", "bmi", "sbp",
  "egfr", "mean_egfr", "k"
)
polr <- c("nyha", "nyha_former")
polyreg <- c("smoking", "smoking_former")

# Define imputation methods
imp_types <- ad_mi2 |>
  names() |>
  as_tibble() |>
  mutate(
    imputation_type = case_when(
      value %in% pmm ~ "pmm",
      value %in% polr ~ "polr",
      value %in% polyreg ~ "polyreg",
      .default = ""
    )
  )

# Multiple imputation
# imp <- mice::mice(
#   ad_mi2, method = imp_types |> pull(imputation_type), pred = pred,
#   m = 5, seed = 700196
# )


# Multiple imputation of heart failure classes ----------------------------

ad_mi_hf_fct <- ad_mi2 |> 
  mutate(hf_fct = if_else(hf_fct == "unspecified", NA, hf_fct))

logreg <- "hf_fct"

# Define imputation methods
imp_types_hf_fct <- ad_mi_hf_fct |>
  names() |>
  as_tibble() |>
  mutate(
    imputation_type = case_when(
      value %in% pmm ~ "pmm",
      value %in% polr ~ "polr",
      value %in% polyreg ~ "polyreg",
      value %in% logreg ~ "logreg",
      .default = ""
    )
  )

# Multiple imputation
imp_hf_fct <- mice::mice(
  ad_mi_hf_fct, method = imp_types_hf_fct |> pull(imputation_type), pred = pred,
  m = 5, seed = 700196
)

write_rds(imp_hf_fct, here::here("output", "multiple_imputation_data_hf_fct.rds"))


# Write data --------------------------------------------------------------

# write_rds(imp, here::here("output", "multiple_imputation_data.rds"))
