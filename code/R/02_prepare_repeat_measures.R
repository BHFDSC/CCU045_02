#*******************************************************************************
#
# Project: CCU005_04
# Title:   Trends and impact of multiple long-term conditions in heart failure
# Date:    12-Mar-2024
# Author:  Robert Fletcher
# Purpose: Prepare repeat measures data for multiple imputation (i.e. biomarkers
#          outside of the window initially examined to create auxilliary
#          variables)
#
#*******************************************************************************


# Notes -------------------------------------------------------------------

# Open the `nicor` RStudio project to run this code


# Load libraries ----------------------------------------------------------

libs <- c("data.table", "here", "lubridate", "tidyverse")

for (l in libs) library(l, character.only = TRUE)


# Source functions --------------------------------------------------------

fns <- c("load_data_locally", "save", "calculate_egfr")

for (f in fns) source(here::here("src", paste0(f, ".R")))


# Define variables --------------------------------------------------------

# Pipeline version
ver <- "v2"


# Load data ---------------------------------------------------------------

# Analysis data
ad <- load_data_locally(.from = "data", .ver = ver, .table = "baseline") |>
  mutate(
    sex = factor(sex_skinny, levels = c(1, 2), labels = c("male", "female"))
  ) |>
  select(
    subject_id, spell_id, admission_date, discharge_date, age, sex, sbp, egfr, 
    smoking, bmi, height_cm, weight_kg, nyha
  ) |>
  distinct()

# NICOR characteristics
nc <- 
  load_data_locally(.from = "data", .ver = ver, .table = "nicor_characteristics")

# GDPPR characteristics
gdc <- 
  load_data_locally(.from = "data", .ver = ver, .table = "gdppr_characteristics") |>
  select(
    -c(reporting_period_end_date, lsoa, practice, record_date, episode_condition, 
       episode_prescription, value1_prescription, value2_prescription, links)
  )


# Prepare weight ----------------------------------------------------------

weight_nc <- ad |> 
  select(
    subject_id, admission_date, discharge_date, height_bl = height_cm, 
    weight_bl = weight_kg
  ) |>
  left_join(
    nc |>
      select(subject_id, date_nicor, matches("^weight_kg")) |>
      filter(if_any(matches("^weight_kg"), \(x) !is.na(x))) |>
      mutate(
        across(matches("weight_kg"), as.numeric),
        across(
          matches("weight_kg"), \(x) if_else(
            x == 0 | x >= 1000, NA, x
          )
        )
      ),
    by = join_by(subject_id)
  ) |>
  mutate(
    weight_kg = case_when(
      is.na(weight_kg_discharge) ~ weight_kg_admission,
      .default = weight_kg_discharge
    )
  ) |>
  select(
    subject_id, admission_date, discharge_date, date = date_nicor, height_bl, 
    weight_bl, weight_kg
  ) |>
  mutate(source = "nicor")

weight_gd <- ad |> 
  select(
    subject_id, admission_date, discharge_date, height_bl = height_cm, 
    weight_bl = weight_kg
  ) |>
  left_join(
    gdc |>
      filter(
        snomed_code == "27113001", !is.na(value1_condition), 
        value1_condition != 0, value1_condition < 1000
      ),
    by = join_by(subject_id)
  ) |>
  select(
    subject_id, admission_date, discharge_date, date, height_bl, weight_bl, 
    weight_kg = value1_condition
  ) |>
  mutate(source = "gdppr")

weight <- 
  bind_rows(weight_nc, weight_gd) |>
  filter(!(is.na(weight_kg) & !is.na(weight_bl))) |>
  arrange(subject_id, date) |>
  distinct()


# Prepare body-mass index (GDPPR only) ------------------------------------

bmi <- ad |> 
  select(subject_id, admission_date, discharge_date, bmi_bl = bmi) |> 
  left_join(
    gdc |>
      filter(
        snomed_code == "60621009", !is.na(value1_condition), 
        !(value1_condition < 10)
      ),
    by = join_by(subject_id)
  ) |>
  select(
    subject_id, admission_date, discharge_date, date, bmi_bl, 
    bmi = value1_condition
  ) |>
  mutate(source = "gdppr")


# Prepare systolic blood pressure -----------------------------------------

sbp_nc <- ad |> 
  select(subject_id, admission_date, discharge_date, sbp_bl = sbp) |>
  left_join(
    nc |>
      select(subject_id, date_nicor, matches("^sbp")) |>
      filter(if_any(matches("^sbp"), \(x) !is.na(x))) |>
      mutate(
        across(matches("^sbp"), \(x) if_else(x == "0", NA, as.double(x))),
        across(matches("^sbp"), \(x) if_else(x < 80 | x > 300, NA, x)),
        sbp = case_when(
          is.na(sbp_admission) ~ sbp_discharge,
          is.na(sbp_discharge) ~ sbp_admission,
          .default = NA
        )
      ) |>
      rowwise() |>
      mutate(
        sbp = if_else(
          is.na(sbp), mean(c_across(c(sbp_admission, sbp_discharge))), sbp
        )
      ) |>
      ungroup(),
    by = join_by(subject_id)
  ) |>
  select(
    subject_id, admission_date, discharge_date, date = date_nicor, sbp_bl, sbp
  ) |>
  mutate(source = "nicor")

sbp_snomed = paste(
  "271649006", "1162737008", "213041000000101", "198081000000101", 
  "251070002", "314438006", "314439003", "314440001", "314464000", 
  "407556006", "407554009", "399304008", "413606001", "716579001", "72313002", 
  "945871000000107", "314449000", "400974009", sep = "|"
)

sbp_gd <- ad |> 
  select(subject_id, admission_date, discharge_date, sbp_bl = sbp) |>
  left_join(
    gdc |>
      filter(str_detect(snomed_code, sbp_snomed), !is.na(value1_condition)) |>
      mutate(
        value1_condition = if_else(
          value1_condition < 80 | value1_condition > 300, NA, value1_condition
        )
      ),
    by = join_by(subject_id)
  )  |>
  select(
    subject_id, admission_date, discharge_date, date, sbp_bl,
    sbp = value1_condition
  ) |>
  mutate(source = "gdppr")

sbp <-
  bind_rows(sbp_nc, sbp_gd) |> 
  filter(!(is.na(sbp) & !is.na(sbp_bl))) |>
  arrange(subject_id, date) |>
  distinct()


# Prepare eGFR ------------------------------------------------------------

gfr_nc <- ad |> 
  select(
    subject_id, admission_date, discharge_date, age, sex, egfr_bl = egfr
  ) |>
  left_join(
    nc |>
      select(subject_id, date_nicor, matches("^creat")) |>
      filter(if_any(matches("^creat"), \(x) !is.na(x))) |>
      mutate(
        across(matches("^creat"), \(x) if_else(x == "0", NA, as.double(x))),
        across(matches("^creat"), \(x) if_else(x >= 10000, x / 1000, x)),
        scr = case_when(
          is.na(creat_admission) ~ creat_discharge,
          is.na(creat_discharge) ~ creat_admission,
          .default = NA
        )
      ) |>
      rowwise() |>
      mutate(
        scr = if_else(
          is.na(scr), mean(c_across(c(creat_admission, creat_discharge))), scr
        )
      ) |>
      ungroup(),
    by = join_by(subject_id)
  ) |>
  calculate_egfr(.scr = scr, .age = age, .sex = sex, .gfr_to = "egfr") |> 
  select(
    subject_id, admission_date, discharge_date, date = date_nicor, egfr_bl, egfr
  ) |>
  mutate(source = "nicor")

gfr_gd <- ad |>
  select(subject_id, admission_date, discharge_date, egfr_bl = egfr) |>
  left_join(
    gdc |>
      filter(
        str_detect(name, "egfr"), !is.na(value1_condition),
        value1_condition != 0
      ),
    by = join_by(subject_id)
  ) |>
  select(
    subject_id, admission_date, discharge_date, date, egfr_bl,
    egfr = value1_condition
  ) |>
  mutate(source = "gdppr")

gfr <- 
  bind_rows(gfr_nc, gfr_gd) |>
  filter(!(is.na(egfr) & !is.na(egfr_bl))) |>
  arrange(subject_id, date) |>
  distinct()


# Prepare smoking ---------------------------------------------------------

smoking_nc <- ad |> 
  select(subject_id, admission_date, discharge_date, smoking_bl = smoking) |>
  left_join(
    nc |>
      select(subject_id, date_nicor, smoking) |>
      filter(!is.na(smoking), !str_detect(smoking, "status unknown|Unk")) |>
      mutate(
        smoking = case_when(
          str_detect(smoking, "Never|Non smoker") ~ "never",
          str_detect(smoking, "Ex") ~ "former",
          str_detect(smoking, "Current|Yes") ~ "current",
          .default = smoking
        )
      ),
    by = join_by(subject_id)
  ) |>
  select(
    subject_id, admission_date, discharge_date, date = date_nicor, smoking_bl,
    smoking
  ) |>
  mutate(source = "nicor")

smoking_gd <- ad |>
  select(subject_id, admission_date, discharge_date, smoking_bl = smoking) |>
  left_join(
    gdc |>
      filter(str_detect(name, "smoking")) |>
      mutate(
        smoking = case_when(
          str_detect(name, "never") ~ "never",
          str_detect(name, "ex") ~ "former",
          str_detect(name, "current") ~ "current",
          .default = name
        )
      ),
    by = join_by(subject_id)
  ) |>
  select(
    subject_id, admission_date, discharge_date, date, smoking_bl, smoking
  ) |>
  mutate(source = "gdppr")

smoking <-
  bind_rows(smoking_nc, smoking_gd) |>
  filter(!(is.na(smoking) & !is.na(smoking_bl))) |>
  arrange(subject_id, date) |>
  distinct()


# Prepare NYHA ------------------------------------------------------------

nyha_nc <- ad |> 
  select(subject_id, admission_date, discharge_date, nyha_bl = nyha) |>
  left_join(
    nc |>
      select(subject_id, date_nicor, breathlessness) |>
      filter(!is.na(breathlessness), !str_detect(breathlessness, "NA$|Unk")) |>
      mutate(
        nyha = case_when(
          str_detect(breathlessness, "[N|n]o limitation") ~ "1",
          str_detect(breathlessness, "[S|s]light limitation") ~ "2",
          str_detect(breathlessness, "[M|m]arked limitation") ~ "3",
          str_detect(breathlessness, "[S|s]ymptoms at rest") ~ "4",
          .default = breathlessness
        )
      ),
    by = join_by(subject_id)
  ) |>
  select(
    subject_id, admission_date, discharge_date, date = date_nicor, nyha_bl, 
    nyha
  ) |>
  mutate(source = "nicor")

nyha_gd <- ad |>
  select(subject_id, admission_date, discharge_date, nyha_bl = nyha) |>
  left_join(
    gdc |>
      filter(str_detect(name, "nyha")) |>
      mutate(
        nyha = case_when(
          str_detect(term, "Class I") ~ "1",
          str_detect(term, "Class II") ~ "2",
          str_detect(term, "Class III") ~ "3",
          str_detect(term, "Class IV") ~ "4",
          .default = term
        )
      ),
    by = join_by(subject_id)
  ) |>
  select(subject_id, admission_date, discharge_date, date, nyha_bl, nyha) |>
  mutate(source = "gdppr")

nyha <-
  bind_rows(nyha_nc, nyha_gd) |>
  filter(!(is.na(nyha) & !is.na(nyha_bl))) |>
  arrange(subject_id, date) |>
  distinct()


# Write data --------------------------------------------------------------

data_list <- lst(weight, bmi, sbp, gfr, smoking, nyha)

for (d in 1:length(data_list)) {
  name <- names(data_list)[d]
  save(data_list[[d]], glue::glue("{name}_repeat_measures"))
} 
