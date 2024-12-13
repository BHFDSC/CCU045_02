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

libs <- c(
  "data.table", "here", "lubridate", "survival", "tidyverse", "mice", "bit64"
)
for (l in libs) library(l, character.only = TRUE)


# Source AF library functions ---------------------------------------------

source(here::here("src", "af_library", "AFcoxph.R"))
source(here::here("src", "af_library", "AFfunctions.R"))


# Source functions --------------------------------------------------------

fns <- c("load_data_locally", "rnd")
for (f in fns) source(here::here("src", paste0(f, ".R")))


# Define functions --------------------------------------------------------

fit_model <- function(.data, .ep = "mortality", .by = NULL) {
  
  if (.ep == "mortality") {
    
    if (missing(.by)) {
      
      fit <- coxph(
        Surv(acm_tte_1yr, acm_ep_1yr) ~ age + sex + ethnic + imd_fct + hf_fct +
          hcv19 + prior_hosp_1yr + care_home + adm_type + smok_known + comorb_fct + hdb + 
          hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
          hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae, 
        data = .data, ties = "breslow"
      )
      
    } else if (.by == "hf_fct") {
      
      fit <- coxph(
        Surv(acm_tte_1yr, acm_ep_1yr) ~ age + sex + ethnic + imd_fct + hcv19 + 
          prior_hosp_1yr + care_home + adm_type + smok_known + comorb_fct + hdb + hhtn + 
          hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune +
          hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae, 
        data = .data, ties = "breslow"
      )
      
    }
    
  } else if (.ep == "hospitalisation") {
    
    if (missing(.by)) {
      
      fit <- coxph(
        Surv(ach_emerg_tte_1yr, ach_emerg_ep_1yr) ~ age + sex + ethnic + imd_fct + hf_fct +
          hcv19 + prior_hosp_1yr + care_home + adm_type + smok_known + comorb_fct + hdb + 
          hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
          hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae, 
        data = .data, ties = "breslow"
      )
      
    } else if (.by == "hf_fct") {
      
      fit <- coxph(
        Surv(ach_emerg_tte_1yr, ach_emerg_ep_1yr) ~ age + sex + ethnic + imd_fct + hcv19 + 
          prior_hosp_1yr + care_home + adm_type + smok_known + comorb_fct + hdb + hhtn + 
          hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune +
          hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae, 
        data = .data, ties = "breslow"
      )
      
    }
    
  }
  
  return(fit)
}

tidy_hrs <- function(.fit) {
  .fit |>
    broom::tidy(exponentiate = TRUE, conf.int = TRUE) |>
    filter(str_detect(term, "^h"), !str_detect(term, "_")) |>
    select(
      comorb = term, hr = estimate, lci = conf.low, uci = conf.high, 
      p.value
    ) |>
    mutate(
      across(
        c(hr, lci, uci), \(x) as.character(rnd(x, 2)), .names = "{.col}_ch"
      ),
      across(
        ends_with("_ch"), \(x) case_when(
          str_detect(x, "[.][0-9]$") ~ paste0(x, "0"),
          !str_detect(x, "[.]") ~ paste0(x, ".00"),
          .default = x
        )
      ),
      res = paste0(hr_ch, " (", lci_ch, "-", uci_ch, ")")
    ) |>
    select(-ends_with("_ch"))
}

get_pafs <- function(.data, .fit) {
  
  extract_paf <- function(.AFcoxph, .comorb_name) {
    
    r <- 
      tibble(
        comorb = .comorb_name,
        paf = .AFcoxph$AF.est,
        paf_se = sqrt(.AFcoxph$AF.var)
      ) |>
      mutate(
        paf_lci = paf - (1.96 * paf_se), paf_uci = paf + (1.96 * paf_se),
        across(starts_with("paf"), \(x) x * 100)
      )
    return(r)
  }
  
  exposures <- c(
    "hcv19", "hdb", "hhtn", "hckd", "hobes", "hihd", "hpad", "haf", 
    "hvalvhd", "hstk", "hautoimmune", "hcanc", "hdem", "hdep", "host", "hthy", 
    "hasthm", "hcopd", "hlivd", "hanae"
  )
  out <- list()
  
  for (i in 1:length(exposures)) {
    res <- AFcoxph(.fit, data = .data, exposure = exposures[i], times = "365.25")
    res2 <- extract_paf(res, exposures[i])
    out[[i]] <- res2
    message(paste(exposures[i], "done."))
  }
  out <- bind_rows(out)
  return(out)
}

get_prevalences <- function(.data, .ep, .tte, .dis) {
  
  prev <- .data |> 
    summarise(
      prev = rnd(sum(!!sym(.dis)) / n_distinct(subject_id) * 100, 1),
      prev_ch = as.character(rnd(sum(!!sym(.dis)) / n_distinct(subject_id) * 100, 1))
    ) |> 
    mutate(
      prev_ch = if_else(
        !str_detect(prev, "[.]"), paste0(prev, ".0%"), paste0(prev, "%")
      )
    )
  
  rate <- .data |>
    summarise(
      count = paste0(
        rnd(sum({{ .ep }}) / 5, 0) * 5, "/", rnd(length({{ .ep }}) / 5, 0) * 5
      ),
      perc = as.character(
        rnd((rnd(sum({{ .ep }}) / 5, 0) * 5) / (rnd(length({{ .ep }}) / 5, 0) * 5) * 100, 1)
      ),
      perc = if_else(!str_detect(perc, "[.]"), paste0(perc, ".0%"), paste0(perc, "%")),
      event = glue::glue("{count} ({perc})"),
      rate = as.character(rnd(sum({{ .ep }}) / (sum({{ .tte }}) / 365.25) * 1000, 1)),
      rate = if_else(!str_detect(rate, "[.]"), paste0(rate, ".0"), rate),
      .by = all_of(.dis)
    ) |>
    mutate(dis := !!sym(.dis)) |> 
    select(dis, event, rate) |>
    pivot_wider(names_from = dis, values_from = c(event, rate)) |>
    mutate(comorb = .dis, .before = 1)
  
  out <- bind_cols(prev, rate) |> 
    relocate(c(prev, prev_ch), .after = "comorb") |> 
    tibble()
  return(out)
}


# Load data ---------------------------------------------------------------

# Baseline data
ad <- load_data_locally(.from = "output", .table = "analysis_data")

# Multiple imputation data
mi <- read_rds(here::here("output", "multiple_imputation_data.rds")) |>
  filter(hhf == 1)


# Prepare data ---------------------------------------------------------------

ad2 <- ad |>
  filter(
    admission_date >= ymd("2019-11-01"), admission_date < ymd("2023-01-01"),
    error == 0, in_hosp_death != 1, !is.na(site), hhf == 1
  ) |>
  mutate(
    across(matches("6mo_after"), \(x) if_else(x == 0, 0, 1)),
    prior_hosp_1yr = if_else(prior_hosp_1yr == 0, 0, as.integer(prior_hosp_1yr / 1)),
    year = format(admission_date, "%Y"),
    year = factor(
      year, levels = c("2019", "2020", "2021", "2022", "2023"), 
      labels = c("2019", "2020", "2021", "2022", "2023")
    ),
    ach_emerg_ep_1yr = case_when(
      ach_emerg_ep == 1 & ach_emerg_tte <= 365.25 ~ 1,
      ach_emerg_ep == 1 & ach_emerg_tte > 365.25 ~ 0,
      ach_emerg_ep == 0 ~ 0, 
      .default = NA
    ),
    across(
      ends_with("emerg_tte"), \(x) case_when(
        x > 365.25 ~ 365.25,
        .default = x
      ), .names = "{.col}_1yr"
    )
  )

# Convert to data.frame because the `AF` library doesn't like tibbles
ad3 <- data.frame(ad2)

# Filter data
ad3_ref <- ad3 |> filter(hf_fct == "hfref")
ad3_pef <- ad3 |> filter(hf_fct == "hfpef")
ad3_spe <- ad3 |> filter(hf_fct != "unspecified")
ad3_uns <- ad3 |> filter(hf_fct == "unspecified")
  

# Get associations and PAFs for comorbidities with death ------------------

# Fit models
fit <- fit_model(ad3)
fit_ref <- fit_model(ad3_ref, .by = "hf_fct")
fit_pef <- fit_model(ad3_pef, .by = "hf_fct")
fit_spe <- fit_model(ad3_spe, .by = "hf_fct")
fit_uns <- fit_model(ad3_uns, .by = "hf_fct")

# Prevalences and crude event rates
exp <- c(
  "hcv19", "hdb", "hhtn", "hckd", "hobes", "hihd", "hpad", "haf", 
  "hvalvhd", "hstk", "hautoimmune", "hcanc", "hdem", "hdep", "host", "hthy", 
  "hasthm", "hcopd", "hlivd", "hanae"
)
prev <- map_df(exp, \(x) get_prevalences(ad3, acm_ep_1yr, acm_tte_1yr, x))
prev_ref <- map_df(exp, \(x) get_prevalences(ad3_ref, acm_ep_1yr, acm_tte_1yr, x))
prev_pef <- map_df(exp, \(x) get_prevalences(ad3_pef, acm_ep_1yr, acm_tte_1yr, x))
prev_spe <- map_df(exp, \(x) get_prevalences(ad3_spe, acm_ep_1yr, acm_tte_1yr, x))
prev_uns <- map_df(exp, \(x) get_prevalences(ad3_uns, acm_ep_1yr, acm_tte_1yr, x))

# Hazard ratios [REMEMBER NEED TO ADD STRATA TERM FOR HOSPITAL SITE]
hr <- tidy_hrs(fit)
hr_ref <- tidy_hrs(fit_ref)
hr_pef <- tidy_hrs(fit_pef)
hr_spe <- tidy_hrs(fit_spe)
hr_uns <- tidy_hrs(fit_uns)

# PAFs
paf <- get_pafs(ad3, fit)
paf_ref <- get_pafs(ad3_ref, fit_ref)
paf_pef <- get_pafs(ad3_pef, fit_pef)
paf_spe <- get_pafs(ad3_spe, fit_spe)
paf_uns <- get_pafs(ad3_uns, fit_uns)

# Combine
dth_all <- reduce(list(prev, hr, paf), left_join, by = join_by(comorb)) |> mutate(group = "All")
dth_ref <- reduce(list(prev_ref, hr_ref, paf_ref), left_join, by = join_by(comorb)) |> mutate(group = "HFrEF")
dth_pef <- reduce(list(prev_pef, hr_pef, paf_pef), left_join, by = join_by(comorb)) |> mutate(group = "HFpEF")
dth_spe <- reduce(list(prev_spe, hr_spe, paf_spe), left_join, by = join_by(comorb)) |> mutate(group = "Specified")
dth_uns <- reduce(list(prev_uns, hr_uns, paf_uns), left_join, by = join_by(comorb)) |> mutate(group = "Unspecified")
dth <- bind_rows(dth_all, dth_ref, dth_pef, dth_spe, dth_uns)


# Get associations and PAFs for comorbidities with hospitalisation --------

# Fit models
fit <- fit_model(ad3, .ep = "hospitalisation")
fit_ref <- fit_model(ad3_ref, .ep = "hospitalisation", .by = "hf_fct")
fit_pef <- fit_model(ad3_pef, .ep = "hospitalisation", .by = "hf_fct")
fit_spe <- fit_model(ad3_spe, .ep = "hospitalisation", .by = "hf_fct")
fit_uns <- fit_model(ad3_uns, .ep = "hospitalisation", .by = "hf_fct")

# Prevalences and crude event rates
prev <- map_df(exp, \(x) get_prevalences(ad3, ach_emerg_ep_1yr, ach_emerg_tte_1yr, x))
prev_ref <- map_df(exp, \(x) get_prevalences(ad3_ref, ach_emerg_ep_1yr, ach_emerg_tte_1yr, x))
prev_pef <- map_df(exp, \(x) get_prevalences(ad3_pef, ach_emerg_ep_1yr, ach_emerg_tte_1yr, x))
prev_spe <- map_df(exp, \(x) get_prevalences(ad3_spe, ach_emerg_ep_1yr, ach_emerg_tte_1yr, x))
prev_uns <- map_df(exp, \(x) get_prevalences(ad3_uns, ach_emerg_ep_1yr, ach_emerg_tte_1yr, x))

# Hazard ratios [REMEMBER NEED TO ADD STRATA TERM FOR HOSPITAL SITE]
hr <- tidy_hrs(fit)
hr_ref <- tidy_hrs(fit_ref)
hr_pef <- tidy_hrs(fit_pef)
hr_spe <- tidy_hrs(fit_spe)
hr_uns <- tidy_hrs(fit_uns)

# PAFs
paf <- get_pafs(ad3, fit)
paf_ref <- get_pafs(ad3_ref, fit_ref)
paf_pef <- get_pafs(ad3_pef, fit_pef)
paf_spe <- get_pafs(ad3_spe, fit_spe)
paf_uns <- get_pafs(ad3_uns, fit_uns)

# Combine
hsp_all <- reduce(list(prev, hr, paf), left_join, by = join_by(comorb)) |> mutate(group = "All")
hsp_ref <- reduce(list(prev_ref, hr_ref, paf_ref), left_join, by = join_by(comorb)) |> mutate(group = "HFrEF")
hsp_pef <- reduce(list(prev_pef, hr_pef, paf_pef), left_join, by = join_by(comorb)) |> mutate(group = "HFpEF")
hsp_spe <- reduce(list(prev_spe, hr_spe, paf_spe), left_join, by = join_by(comorb)) |> mutate(group = "Specified")
hsp_uns <- reduce(list(prev_uns, hr_uns, paf_uns), left_join, by = join_by(comorb)) |> mutate(group = "Unspecified")
hsp <- bind_rows(hsp_all, hsp_ref, hsp_pef, hsp_spe, hsp_uns)


# PAFs in multiple imputation data ----------------------------------------

mi_spe <- filter(mi, str_detect(hf_fct, "hfpef|hfref"))
mi_ref <- filter(mi, str_detect(hf_fct, "hfref"))
mi_pef <- filter(mi, str_detect(hf_fct, "hfpef"))

get_mi_hazard_ratios_mortality <- function(.data, .endpoint, .group) {
  
  prep <- function(.fit) {
    .fit |> 
      pool() |>
      broom::tidy(exponentiate = TRUE, conf.int = TRUE) |>
      tibble() |>
      select(
        term, estimate, std.error, statistic, p.value, conf.low, conf.high
      )
  }
  
  if (.endpoint == "mortality") {
    all <- with(
      .data, 
      coxph(
        Surv(acm_tte_1yr, acm_ep_1yr) ~ age + sex + ethnic + imd_fct + hf_fct +
          hcv19 + prior_hosp_1yr + care_home + adm_type + comorb_fct + hdb + 
          hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
          hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae + sbp + 
          egfr + bmi + smoking + nyha + strata(site)
      ) 
    ) |>
      prep() |>
      filter(str_detect(term, "^h"), !str_detect(term, "_|hckd|hhtn|hobes"))
    
    ckd <- with(
      .data, 
      coxph(
        Surv(acm_tte_1yr, acm_ep_1yr) ~ age + sex + ethnic + imd_fct + hf_fct +
          hcv19 + prior_hosp_1yr + care_home + adm_type + comorb_fct + hdb + 
          hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
          hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae + sbp + 
          bmi + smoking + nyha + strata(site)
      ) 
    ) |> 
      prep() |>
      filter(str_detect(term, "hckd"))
    
    htn <- with(
      .data, 
      coxph(
        Surv(acm_tte_1yr, acm_ep_1yr) ~ age + sex + ethnic + imd_fct + hf_fct +
          hcv19 + prior_hosp_1yr + care_home + adm_type + comorb_fct + hdb + 
          hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
          hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae +  
          egfr + bmi + smoking + nyha + strata(site)
      ) 
    ) |> 
      prep() |>
      filter(str_detect(term, "hhtn"))
    
    obes <- with(
      .data, 
      coxph(
        Surv(acm_tte_1yr, acm_ep_1yr) ~ age + sex + ethnic + imd_fct + hf_fct +
          hcv19 + prior_hosp_1yr + care_home + adm_type + comorb_fct + hdb + 
          hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
          hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae + sbp + 
          egfr + smoking + nyha + strata(site)
      ) 
    ) |> 
      prep() |>
      filter(str_detect(term, "hobes"))
  } else if (.endpoint == "hospitalisation") {
    all <- with(
      .data, 
      coxph(
        Surv(ach_emerg_tte_1yr, ach_emerg_ep_1yr) ~ age + sex + ethnic + imd_fct + hf_fct +
          hcv19 + prior_hosp_1yr + care_home + adm_type + comorb_fct + hdb + 
          hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
          hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae + sbp + 
          egfr + bmi + smoking + nyha + strata(site)
      ) 
    ) |>
      prep() |>
      filter(str_detect(term, "^h"), !str_detect(term, "_|hckd|hhtn|hobes"))
    
    ckd <- with(
      .data, 
      coxph(
        Surv(ach_emerg_tte_1yr, ach_emerg_ep_1yr) ~ age + sex + ethnic + imd_fct + hf_fct +
          hcv19 + prior_hosp_1yr + care_home + adm_type + comorb_fct + hdb + 
          hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
          hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae + sbp + 
          bmi + smoking + nyha + strata(site)
      ) 
    ) |> 
      prep() |>
      filter(str_detect(term, "hckd"))
    
    htn <- with(
      .data, 
      coxph(
        Surv(ach_emerg_tte_1yr, ach_emerg_ep_1yr) ~ age + sex + ethnic + imd_fct + hf_fct +
          hcv19 + prior_hosp_1yr + care_home + adm_type + comorb_fct + hdb + 
          hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
          hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae +  
          egfr + bmi + smoking + nyha + strata(site)
      ) 
    ) |> 
      prep() |>
      filter(str_detect(term, "hhtn"))
    
    obes <- with(
      .data, 
      coxph(
        Surv(ach_emerg_tte_1yr, ach_emerg_ep_1yr) ~ age + sex + ethnic + imd_fct + hf_fct +
          hcv19 + prior_hosp_1yr + care_home + adm_type + comorb_fct + hdb + 
          hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
          hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae + sbp + 
          egfr + smoking + nyha + strata(site)
      ) 
    ) |> 
      prep() |>
      filter(str_detect(term, "hobes"))
  }
  
  res <- bind_rows(all, ckd, obes, htn) |>
    arrange(desc(estimate)) |> select(
    comorb = term, hr = estimate, lci = conf.low, uci = conf.high, 
    p.value
  ) |>
    mutate(
      across(
        c(hr, lci, uci), \(x) as.character(rnd(x, 2)), .names = "{.col}_ch"
      ),
      across(
        ends_with("_ch"), \(x) case_when(
          str_detect(x, "[.][0-9]$") ~ paste0(x, "0"),
          !str_detect(x, "[.]") ~ paste0(x, ".00"),
          .default = x
        )
      ),
      res = paste0(hr_ch, " (", lci_ch, "-", uci_ch, ")")
    ) |>
    select(-ends_with("_ch")) |>
    mutate(group = .group)
  return(res)
}

# Death
res_dth_mi <- get_mi_hazard_ratios_mortality(mi, "mortality", "All")
res_dth_mi_spe <- get_mi_hazard_ratios_mortality(mi_spe, "mortality", "Specified")
res_dth_mi_ref <- get_mi_hazard_ratios_mortality(mi_ref, "mortality", "HFrEF")
res_dth_mi_pef <- get_mi_hazard_ratios_mortality(mi_pef, "mortality", "HFpEF")

res_dth_mi_ttl <- 
  bind_rows(res_dth_mi, res_dth_mi_spe, res_dth_mi_ref, res_dth_mi_pef) |>
  mutate(endpoint = "Mortality")

# Re-hospitalisation
res_hsp_mi <- get_mi_hazard_ratios_mortality(mi, "hospitalisation", "All") 
res_hsp_mi_spe <- get_mi_hazard_ratios_mortality(mi_spe, "hospitalisation", "Specified")
res_hsp_mi_ref <- get_mi_hazard_ratios_mortality(mi_ref, "hospitalisation", "HFrEF")
res_hsp_mi_pef <- get_mi_hazard_ratios_mortality(mi_pef, "hospitalisation", "HFpEF")

res_hsp_mi_ttl <- 
  bind_rows(res_hsp_mi, res_hsp_mi_spe, res_hsp_mi_ref, res_hsp_mi_pef) |>
  mutate(endpoint = "Hospitalisation")

res_mi <- bind_rows(res_dth_mi_ttl, res_hsp_mi_ttl)


# Write data --------------------------------------------------------------

write_csv(dth, here::here("figs", "comorbidities", "09_death_comorbidities_paf_chronichf.csv"))
write_csv(hsp, here::here("figs", "comorbidities", "10_hospitalisation_comorbidities_paf_chronichf.csv"))
write_csv(res_mi, here::here("figs", "comorbidities", "11_mi_results_comorbidities_chronichf.csv"))
