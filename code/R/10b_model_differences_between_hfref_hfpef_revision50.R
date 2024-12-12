#*******************************************************************************
#
# Project: CCU005_04
# Title:   Trends and impact of multiple long-term conditions in heart failure
# Date:    22-Mar-2024
# Author:  Robert Fletcher
# Purpose: Model all-cause and cause-specific deaths
#
#*******************************************************************************


# Notes -------------------------------------------------------------------

# Open the `nicor` RStudio project to run this code


# Load libraries ----------------------------------------------------------

libs <- c("here", "lubridate", "survival", "tidyverse", "mice", "bit64")
for (l in libs) library(l, character.only = TRUE)


# Source functions --------------------------------------------------------

fns <- c("load_data_locally", "rnd")
for (f in fns) source(here::here("src", paste0(f, ".R")))


# Load data ---------------------------------------------------------------

# Baseline data
ad <- load_data_locally(.from = "output", .table = "analysis_data")

# Multiple imputation data
mi <- read_rds(here::here("output", "multiple_imputation_data.rds"))

# Hospitals mapping
hosp <- load_data_locally(
  .from = "data", .ver = ver, .table = "hospitals_nhfa_mapping_percentage"
) |>
  filter(perc_mapped_pri >= 50)


# Prepare data ------------------------------------------------------------

ad2 <- ad |>
  filter(
    admission_date >= ymd("2019-11-01"), admission_date < ymd("2023-01-01"),
    error == 0, in_hosp_death != 1, !is.na(site)
  ) |>
  mutate(
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
    across(matches("6mo_after"), \(x) if_else(x == 0, 0, 1)),
    prior_hosp_1yr = if_else(prior_hosp_1yr == 0, 0, as.integer(prior_hosp_1yr / 1)),
    year = format(admission_date, "%Y"),
    year = factor(
      year, levels = c("2019", "2020", "2021", "2022", "2023"), 
      labels = c("2019", "2020", "2021", "2022", "2023")
    ),
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
  # Filter hospitals
  filter(if_any(starts_with("site"), \(x) x %in% hosp$hospital_site_code))

mi <- mi |>
  # Filter hospitals
  filter(if_any(starts_with("site"), \(x) x %in% hosp$hospital_site_code))

# Get people who are COVID-19 naive
ad2_no_hcv19 <- ad2 |> filter(hcv19 == 0)
mi_no_hcv19 <- mi |> filter(hcv19 == 0)


# Model deaths ------------------------------------------------------------

model_mortality <- function(.data, .ep, .tte, .mi_obj, .mi_ep, .mi_tte, .ep_title) {
  
  get_events <- function(.data) {
    ev <- .data |>
      summarise(
        count = paste0(
          rnd(sum({{ .ep }}) / 5, 0) * 5, "/", rnd(length({{ .ep }}) / 5, 0) * 5
        ),
        perc = as.character(
          rnd((rnd(sum({{ .ep }}) / 5, 0) * 5) / (rnd(length({{ .ep }}) / 5, 0) * 5) * 100, 1)
        ),
        #count = paste0(sum({{ .ep }}), "/", length({{ .ep }})),
        #perc = as.character(rnd(sum({{ .ep }}) / length({{ .ep }}) * 100, 1)),
        perc = if_else(!str_detect(perc, "[.]"), paste0(perc, ".0"), perc),
        event = glue::glue("{count} ({perc})"),
        rate = as.character(rnd(sum({{ .ep }}) / (sum({{ .tte }}) / 365.25) * 1000, 1)),
        rate = if_else(!str_detect(rate, "[.]"), paste0(rate, ".0"), rate),
        .by = hf_subtype
      ) |>
      mutate(
        group = factor(
          hf_subtype, levels = c("hfref", "hfpef", "unspecified"),
          labels = c("Reduced ejection fraction", "Preserved ejection fraction",
                     "Unknown")
        )
      ) |>
      select(group, event, rate) |>
      arrange(group)
  }
  
  get_model_results <- function(.data, .adjustment) {
    
    if (.adjustment == "min") {
      fit <- coxph(
          Surv(tte, ep) ~ age + sex + ethnic + imd_fct + hf_fct +
            hcv19 + prior_hosp_1yr + care_home + hhf + adm_type + smok_known +
            comorb_fct + hdb + hhtn + hckd + hobes + hihd + hpad + haf + 
            hvalvhd + hstk + hautoimmune + hcanc + hdem + hdep + host + hthy + 
            hasthm + hcopd + hlivd + hanae + strata(site), data = .data
        ) |>
        broom::tidy(exponentiate = TRUE, conf.int = TRUE)
    } else if (.adjustment == "max") {
      fit <- coxph(
          Surv(tte, ep) ~ age + sex + ethnic + imd_fct + hf_fct +
            hcv19 + prior_hosp_1yr + care_home + hhf + adm_type + comorb_fct + 
            hdb + hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + 
            hautoimmune + hcanc + hdem + hdep + host + hthy + hasthm + hcopd + 
            hlivd + hanae + sbp + bmi + egfr + smoking + nyha + strata(site), 
          data = .data
        ) |>
        broom::tidy(exponentiate = TRUE, conf.int = TRUE)
    } else if (.adjustment == "mi") {
      fit <- with(
        .mi_obj, 
        coxph(
          as.formula(
            paste0(
              "Surv(", .mi_tte, ", ", .mi_ep, 
              ") ~ age + sex + ethnic + imd_fct + hf_fct + hcv19 + prior_hosp_1yr + care_home +",
              "hhf + adm_type + comorb_fct + hdb + hhtn + hckd + hobes + hihd +",
              "hpad + haf + hvalvhd + hstk + hautoimmune + hcanc + hdem + hdep +",
              "host + hthy + hasthm + hcopd + hlivd + hanae + sbp + bmi + egfr +",
              "smoking + nyha + strata(site)"
            )
          )
        )
      )
      fit <- pool(fit) |>
        broom::tidy(exponentiate = TRUE, conf.int = TRUE) |> 
        tibble() |> 
        select(term, estimate, std.error, statistic, p.value, conf.low, conf.high)
    }
    
    fit <- fit |> 
      filter(str_detect(term, "hf_fct")) |>
      rename(hr = estimate, lci = conf.low, uci = conf.high) |>
      mutate(
        term = case_when(
          str_detect(term, "hfpef") ~ "Preserved ejection fraction",
          str_detect(term, "unspec") ~ "Unknown",
        ),
        across(
          c(hr, lci, uci), \(x) as.character(rnd(x, decimals = 2)),
          .names = "{.col}_char"
        ),
        across(
          ends_with("char"), \(x) case_when(
            str_detect(x, "^[0-9]{1,2}[.][0-9]$") ~ paste0(x, "0"),
            !str_detect(x, "[.]") ~ paste0(x, ".00"),
            .default = x
          )
        ),
        result = paste0(hr_char, " (", lci_char, " to ", uci_char, ")")
      ) |>
      select(-ends_with("char")) |>
      select(group = term, hr, lci, uci, result, p = p.value) |>
      add_row(
        group = "Reduced ejection fraction", hr = 1, lci = 1, uci = 1, 
        result = "Reference", .before = 1
      ) |>
      mutate(
        group = factor(
          group, 
          levels = c("Reduced ejection fraction", "Preserved ejection fraction", "Unknown"),
          labels = c("Reduced ejection fraction", "Preserved ejection fraction", "Unknown")
        )
      )
    return(fit)
  }
  
  # Rename endpoint variable
  data <- .data |>
    mutate(ep = {{ .ep }}, tte = {{ .tte }})
  
  # Whole dataset
  ns_min <- data |> get_events()
  fit_min <- data |> get_model_results(.adjustment = "min")
  res_min <- ns_min |>
    left_join(fit_min, by = join_by(group)) |>
    mutate(type = "Minimally-adjusted, full dataset") |>
    add_row(group = .ep_title, .before = 1)
  
  # Reduced dataset
  data_red <- data |> drop_na(sbp, bmi, egfr, smoking, nyha)
  ns_min_red <- data_red |> get_events()
  fit_min_red <- data_red |> get_model_results(.adjustment = "min")
  fit_max <- data |> get_model_results(.adjustment = "max")
  res_min_red <- ns_min_red |>
    left_join(fit_min_red, by = join_by(group)) |>
    mutate(type = "Minimally-adjusted, reduced dataset") |>
    arrange(group)
  res_max <- ns_min_red |>
    left_join(fit_max, by = join_by(group)) |>
    mutate(type = "Maximally-adjusted") |>
    arrange(group)
  
  # MI dataset
  fit_mi <- get_model_results(.adjustment = "mi")
  res_mi <- ns_min |>
    left_join(fit_mi, by = join_by(group)) |>
    mutate(type = "Multiple imputation")
   
  res <-
    bind_rows(res_min, res_min_red, res_max, res_mi) |>
    mutate(endpoint = .ep_title)
  
  return(res)
  message("Run complete.")
}

eps <- tribble(
  ~ep, ~name,
  "acm_ep_1yr", "All-cause mortality",
  "cvd_ep_1yr", "Cardiovascular death",
  "non_cvd_ep_1yr", "Non-cardiovascular death",
  "hfd_ep_1yr", "Death due to heart failure",
  "mid_ep_1yr", "Fatal myocardial infarction",
  "strkd_ep_1yr", "Fatal stroke",
  "kidd_ep_1yr", "Death due to kidney failure",
  "covidd_ep_1yr", "Death due to COVID-19",
  "infd_ep_1yr", "Death due to infection (excl. COVID-19)",
  "cancd_ep_1yr", "Death due to cancer/malignancy",
  "neurod_ep_1yr", "Death from neurological disorders"
)
death_res <- list()

for (e in 1:nrow(eps)) {
  res <- model_mortality(
    ad2, !!sym(eps$ep[e]), !!sym("acm_tte_1yr"), mi, eps$ep[e], "acm_tte_1yr",
    eps$name[e]
  )
  death_res[[e]] <- res
}
death_res <- death_res |> bind_rows() 

# COVID-19 death for naive people
death_res_cv19 <- model_mortality(
  ad2_no_hcv19, covidd_ep_1yr, acm_tte_1yr, mi_no_hcv19, "covidd_ep_1yr", "acm_tte_1yr",
  "Death due to COVID-19"
)

death_res2 <- bind_rows(death_res, death_res_cv19)

hsp <- tribble(
  ~ep, ~tte, ~name,
  "ach_emerg_ep_1yr", "ach_emerg_tte_1yr", "All-cause hospitalisation",
  "hcv_pri_emerg_ep_1yr", "hcv_pri_emerg_tte_1yr", "Cardiovascular hospitalisation",
  "hncv_pri_emerg_ep_1yr", "hncv_pri_emerg_tte_1yr", "Non-cardiovascular hospitalisation",
  "hhf_pri_emerg_ep_1yr", "hhf_pri_emerg_tte_1yr", "Heart failure hospitalisation"
)
hsp_res <- list()

# Whole cohort
for (e in 1:nrow(hsp)) {
  res <- model_mortality(
    ad2, !!sym(hsp$ep[e]), !!sym(hsp$tte[e]), mi, hsp$ep[e], hsp$tte[e],
    hsp$name[e]
  )
  hsp_res[[e]] <- res
}
hsp_res <- hsp_res |> bind_rows() 

# COVID-19 death for naive people
hosp_res_cv19 <- model_mortality(
  ad2_no_hcv19, hcv19_pri_emerg_ep_1yr, hcv19_pri_emerg_tte_1yr, mi_no_hcv19,
  "hcv19_pri_emerg_ep_1yr", "hcv19_pri_emerg_tte_1yr",
  "Hospitalisation for COVID-19"
)

hsp_res2 <- bind_rows(hsp_res, hosp_res_cv19)


# Write data --------------------------------------------------------------

death_res2 |>
  write_csv(here::here("figs", "subclass_comparison", "revision_hosp50", "01_deaths_comparison_revision50.csv"))
hsp_res2 |>
  write_csv(here::here("figs", "subclass_comparison", "revision_hosp50", "02_hosp_comparison_revision50.csv"))

