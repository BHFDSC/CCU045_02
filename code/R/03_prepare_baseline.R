#*******************************************************************************
#
# Project: CCU005_04
# Title:   Trends and impact of multiple long-term conditions in heart failure
# Date:    12-Mar-2024
# Author:  Robert Fletcher
# Purpose: Prepare baseline data
#
#*******************************************************************************


# Notes -------------------------------------------------------------------

# Open the `nicor` RStudio project to run this code


# Load libraries ----------------------------------------------------------

libs <- c("data.table", "here", "lubridate", "tidyverse")

for (l in libs) library(l, character.only = TRUE)


# Source functions --------------------------------------------------------

fns <- c("load_data_locally", "calculate_egfr", "save")

for (f in fns) source(here::here("src", paste0(f, ".R")))


# Define variables --------------------------------------------------------

# Pipeline version
ver <- "v2"


# Load data ---------------------------------------------------------------

# Baseline data
bl <- load_data_locally(.from = "data", .ver = ver, .table = "baseline")

# COVID testing data
cnfrm_cv19 <- 
  load_data_locally(.from = "data", .ver = ver, .table = "confirmed_covid")
pillar2 <- 
  load_data_locally(.from = "data", .ver = ver, .table = "pillar2_clean")

# Repeat measures data
gfr <- load_data_locally(.from = "output", .table = "gfr_repeat_measures")


# Join confirmed COVID-19 and pillar 2 test data --------------------------

pos_tests <- "Positive|1322781000000102$|1240581000000104$"

pillar2_cln <- pillar2 |>
  mutate(
    covid_phenotype = case_when(
      str_detect(TestResult, pos_tests) ~ "01_Covid_positive_test",
      .default = "neg"
    ),
    clinical_code = "", description = "", covid_status = "", code = "",
    source = "pillar2"
  ) |>
  filter(covid_phenotype != "neg") |>
  select(
    subject_id, DATE = TestStartDate, covid_phenotype, clinical_code,
    description, covid_status, code, source
  ) |> 
  distinct()

cnfrm_cv19_2 <- cnfrm_cv19 |>
  bind_rows(pillar2_cln) |>
  distinct() |>
  arrange(subject_id, DATE) |>
  rename_with(tolower)


# Quality control ---------------------------------------------------------

# Exclude patients <18 years
bl2 <- bl |> 
  filter(age >= 18)

# Identify duplicates
dup <- bl2 |>
  group_by(subject_id) |>
  filter(n() > 1) |>
  ungroup() |>
  distinct(subject_id)


# Remove duplicates
bl3 <- bl2 |>
  group_by(subject_id) |>
  fill(
    c(lsoa_residence, imd_2019_quintiles, imd_2019_deciles), 
    .direction = "updown"
  ) |>
  ungroup() |>
  distinct()


# Define history of COVID-19 ----------------------------------------------

cv19_pos <- bl2 |>
  select(subject_id, discharge_date, hcv19) |> 
  left_join(cnfrm_cv19_2, by = join_by(subject_id)) |>
  filter(
    date <= discharge_date, date >= "2020-01-01", covid_status != "suspected"
  ) |>
  select(-c(discharge_date, hcv19))

cv <- bl2 |>
  select(subject_id, discharge_date, hcv19) |> 
  left_join(cv19_pos, by = join_by(subject_id)) |>
  group_by(subject_id) |>
  arrange(date) |>
  mutate(
    hcv19_2 = case_when(
      !is.na(covid_phenotype) ~ 1,
      .default = NA
    ),
    hcv19_hosp = case_when(
      str_detect(covid_phenotype, "Covid_admission") ~ 1,
      .default = NA
    ),
    hcv19_earliest = case_when(
      !is.na(date) & row_number() == 1 ~ date,
      .default = NA
    )
  ) |>
  fill(c(hcv19_2, hcv19_hosp, hcv19_earliest), .direction = "updown") |>
  ungroup() |>
  select(subject_id, hcv19 = hcv19_2, hcv19_hosp, hcv19_earliest) |>
  distinct() |>
  mutate(across(c(hcv19, hcv19_hosp), \(x) if_else(is.na(x), 0, x)))


# Clean data --------------------------------------------------------------

bl4 <- bl3 |>
  select(-hcv19) |>
  left_join(cv, by = join_by(subject_id)) |>
  mutate(
    hf_subtype = if_else(is.na(hf_subtype), "unspecified", hf_subtype),
    subclass_source = case_when(
      hf_subtype == "unspecified" & hdicard == 1 ~ "icd-10", 
      .default = subclass_source
    ),
    hf_subtype = case_when(
      subclass_source == "icd-10" ~ "hfref",
      .default = hf_subtype
    ),
    hf_fct = factor(
      hf_subtype, levels = c("hfref", "hfpef", "unspecified"),
      labels = c("hfref", "hfpef", "unspecified")
    ),
    adm_type = case_when(
      str_detect(admission_method, "^1") ~ "elective",
      str_detect(admission_method, "^2") ~ "emergency",
      str_detect(admission_method, "^3|^8|^9") ~ "maternity/other/unknown",
      .default = NA
    ),
    adm_type = factor(
      adm_type, levels = c("elective", "emergency", "maternity/other/unknown"),
      labels = c("elective", "emergency", "maternity/other/unknown")
    ),
    in_hosp_death = case_when(discharge_method == 4 ~ 1, .default = 0),
    sex = factor(sex_skinny, levels = c(1, 2), labels = c("male", "female")),
    ethnic = case_when(
      str_detect(ethnicity_cat_skinny, "Black") ~ "black",
      str_detect(ethnicity_cat_skinny, "Asian") ~ "asian",
      str_detect(ethnicity_cat_skinny, "Wh|Mi|Oth") ~ tolower(ethnicity_cat_skinny),
      str_detect(ethnicity_cat_skinny, "Unk") ~ "missing",
      is.na(ethnicity_cat_skinny) ~ "missing",
      .default = NA
    ),
    ethnic = factor(
      ethnic, levels = c("white", "black", "asian", "mixed", "other", "missing"),
      labels = c("white", "black", "asian", "mixed", "other", "missing")
    ),
    smok_fct = if_else(is.na(smoking), "missing", as.character(smoking)),
    smok_fct = factor(
      smok_fct, levels = c("current", "former", "never", "missing"),
      labels = c("current", "former", "never", "missing"),
    ),
    smok_known = if_else(smoking %in% c("current", "former"), 1, 0),
    imd_fct = if_else(
      is.na(imd_2019_quintiles), "Missing", as.character(imd_2019_quintiles)
    ),
    imd_fct = factor(
      imd_fct, levels = c("1", "2", "3", "4", "5", "Missing"),
      labels = c(
        "1 (Most deprived)", "2", "3", "4", "5 (Least deprived)", "Missing"
      )
    ),
    imd_q10_fct = if_else(
      is.na(imd_2019_deciles), "Missing", as.character(imd_2019_deciles)
    ),
    imd_q10_fct = factor(
      imd_q10_fct, 
      levels = c("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "Missing"),
      labels = c("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "Missing"),
    ),
    nyha_fct = if_else(is.na(nyha), "Missing", as.character(nyha)),
    nyha_fct = factor(
      nyha_fct, levels = c("1", "2", "3", "4", "Missing"),
      labels = c("I", "II", "III", "IV", "Missing"),
    ),
    across(matches("height|weight|sbp|scr|^k$|bnp$"), \(x) abs(as.double(x))),
    # Including this because `weight_date` converts to numeric
    across(matches("_date") & where(is.numeric), as_date),
    # Process body-mass index
    bmi2 = case_when(
      is.na(bmi) & !is.na(height_cm) & 
        !is.na(weight_kg) ~ weight_kg / (height_cm / 100)^2,
      .default = bmi
    ),
    bmi2 = weight_kg / (height_cm / 100)^2,
    across(c("bmi", "bmi2"), \(x) if_else(x < 10 | x > 120, NA, x)),
    diff_weight = abs(difftime(admission_date, weight_date, units = "days")),
    diff_bmi = abs(difftime(admission_date, bmi_date, units = "days")),
    bmi3 = case_when(
      is.na(bmi) & !is.na(bmi2) ~ bmi2,
      bmi == bmi2 ~ bmi,
      !is.na(bmi) & !is.na(bmi2) & (diff_weight > diff_bmi) ~ bmi,
      !is.na(bmi) & !is.na(bmi2) & (diff_weight < diff_bmi) ~ bmi2,
      .default = bmi
    ),
    bmi_fct = case_when(
      bmi3 < 18.5 ~ 1, 
      bmi3 >= 18.5 & bmi3 < 25 ~ 2,
      bmi3 >= 25 & bmi3 < 30 ~ 3,
      bmi3 >= 30 ~ 4,
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
    sbp_fct = factor(
      sbp_fct, levels = c(1:5),
      labels = c("<120", "120 to 129", "130 to 139", "140 to 149", ">=150")
    )
  ) |>
  calculate_egfr(.scr = scr, .age = age, .sex = sex, .gfr_to = "egfr2") |>
  mutate(
    across(c("egfr", "egfr2"), \(x) if_else(x > 200, NA, x)),
    diff_egfr = abs(difftime(admission_date, egfr_date, units = "days")),
    diff_scr = abs(difftime(admission_date, scr_date, units = "days")),
    egfr3 = case_when(
      is.na(egfr) & !is.na(egfr2) ~ egfr2,
      egfr == egfr2 ~ egfr,
      egfr < 5 & egfr2 > 5 ~ egfr2,
      egfr2 < 5 & egfr > 5 ~ egfr,
      !is.na(egfr) & !is.na(egfr2) & (diff_egfr < diff_scr) ~ egfr,
      !is.na(egfr) & !is.na(egfr2) & (diff_egfr > diff_scr) ~ egfr2,
      .default = egfr
    ),
    across(c(hhf:hautoimmune), as.numeric)
  ) |>
  select(-matches("bmi$|bmi2|egfr$|egfr2|diff_|_skinny")) |>
  rename(bmi = bmi3, egfr = egfr3) |>
  # Add BMI to definition of obesity
  mutate(
    hobes_prev = hobes,
    hobes = case_when(
      hobes == 0 & bmi >= 30 ~ 1,
      .default = hobes
    )
  ) |>
  relocate(harryth, .after = "hautoimmune") |> 
  rowwise() |>
  # Sum number of comorbidities
  mutate(comorb_num = sum(c_across(hdb:hautoimmune))) |> 
  ungroup() |>
  mutate(
    comorb_fct = case_when(
      comorb_num >= 5 ~ "≥5",
      .default = as.character(comorb_num)
    ),
    comorb_fct = factor(
      comorb_fct, levels = c("0", "1", "2", "3", "4", "≥5"),
      labels = c("0", "1", "2", "3", "4", "≥5")
    ),
    comorb_far = case_when(
      hf_subtype == "hfref" & comorb_fct == "2" ~ 1,
      hf_subtype == "hfpef" & comorb_fct == "2" ~ 2,
      hf_subtype == "hfref" & comorb_fct == "1" ~ 3,  
      hf_subtype == "hfpef" & comorb_fct == "1" ~ 4,  
      hf_subtype == "hfref" & comorb_fct == "0" ~ 5, 
      hf_subtype == "hfpef" & comorb_fct == "0" ~ 6, 
      hf_subtype == "hfref" & comorb_fct == "3" ~ 7, 
      hf_subtype == "hfpef" & comorb_fct == "3" ~ 8, 
      hf_subtype == "hfref" & comorb_fct == "4" ~ 9, 
      hf_subtype == "hfpef" & comorb_fct == "4" ~ 10, 
      hf_subtype == "hfref" & comorb_fct == "=5" ~ 11, 
      hf_subtype == "hfpef" & comorb_fct == "=5" ~ 12,
      hf_subtype == "unspecified" & comorb_fct == "0" ~ 13,
      hf_subtype == "unspecified" & comorb_fct == "1" ~ 14,
      hf_subtype == "unspecified" & comorb_fct == "2" ~ 15,
      hf_subtype == "unspecified" & comorb_fct == "3" ~ 16,
      hf_subtype == "unspecified" & comorb_fct == "4" ~ 17,
      hf_subtype == "unspecified" & comorb_fct == "=5" ~ 18
    ),
    comorb_far = factor(
      comorb_far, levels = c(1:18),
      labels = c(
        "hfref2", "hfpef2", "hfref1", "hfpef1", "hfref0", "hfpef0", 
        "hfref3", "hfpef3", "hfref4", "hfpef4", "hfref5+", "hfpef5+", 
        "uns0", "uns1", "uns2", "uns3", "uns4", "uns5+"
      )
    ),
    site = case_when(
      !is.na(site_3) ~ site_3,
      !is.na(site_2) & is.na(site_3) ~ site_2,
      !is.na(site_1) & is.na(site_2) ~ site_1
    )
  )


# Add historic eGFR for defining CKD --------------------------------------

# At least two measurements of eGFR < 60 at least 90 days apart
ckd <- gfr |>
  filter(date <= admission_date, egfr < 60) |>
  mutate(diff = as.numeric(admission_date - date) / 365.25) |>
  arrange(subject_id, date) |> 
  group_by(subject_id) |>
  mutate(diff_meas = as.numeric(date - first(date))) |>
  ungroup() |>
  filter(diff_meas >= 90)

bl5 <- bl4 |>
  mutate(hckd = if_else(hckd == 0 & subject_id %in% ckd$subject_id, 1, hckd))


# Write data --------------------------------------------------------------

save(bl5, "baseline")
save(cnfrm_cv19_2, "covid19_diagnoses")
