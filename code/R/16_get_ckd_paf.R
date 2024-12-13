#*******************************************************************************
#
# Project: CCU005_04
# Title:   Trends and impact of multiple long-term conditions in heart failure
# Date:    22-Mar-2024
# Author:  Robert Fletcher
# Purpose: Compute population attributable fraction for CKD in multiple 
#          imputation data
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
    "hcv19", "hhf", "hdb", "hhtn", "hckd", "hobes", "hihd", "hpad", "haf", 
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


# Load data ---------------------------------------------------------------

# Multiple imputation data
mi <- read_rds(here::here("output", "multiple_imputation_data.rds"))


# Get PAF for CKD ---------------------------------------------------------

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

get_paf_mi <- function(.mi, .by = NULL, .endpoint, .comorb) {
  one <- complete(.mi, 1)
  two <- complete(.mi, 2)
  thr <- complete(.mi, 3)
  fou <- complete(.mi, 4)
  fiv <- complete(.mi, 5)
  ls <- list(one, two, thr, fou, fiv)
  
  if (missing(.by)) {
    ls <- ls
  } else if (.by %in% c("hfref", "hfpef", "unspecified")) {
    ls <- map(ls, \(x) filter(x, str_detect(hf_fct, .by)))
  } else if (.by == "specified") {
    ls <- map(ls, \(x) filter(x, hf_fct != "unspecified"))
  }
  
  if (.endpoint == "mortality") {
    if (missing(.by)) {
      res_ls <- map(
        ls, \(x) coxph(
          Surv(acm_tte_1yr, acm_ep_1yr) ~ age + sex + ethnic + imd_fct + hf_fct +
            hcv19 + prior_hosp_1yr + care_home + hhf + adm_type + comorb_fct + hdb + 
            hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
            hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae + sbp + 
            bmi + smoking + nyha, data = x, ties = "breslow"
        )
      )
    } else if (.by %in% c("hfref", "hfpef", "unspecified", "specified")) {
      res_ls <- map(
        ls, \(x) coxph(
          Surv(acm_tte_1yr, acm_ep_1yr) ~ age + sex + ethnic + imd_fct +
            hcv19 + prior_hosp_1yr + care_home + hhf + adm_type + comorb_fct + hdb + 
            hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
            hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae + sbp + 
            bmi + smoking + nyha, data = x, ties = "breslow"
        )
      )
    } 
  } else if (.endpoint == "hospitalisation") {
    if (missing(.by)) {
      res_ls <- map(
        ls, \(x) coxph(
          Surv(ach_emerg_tte_1yr, ach_emerg_ep_1yr) ~ age + sex + ethnic + imd_fct + hf_fct +
            hcv19 + prior_hosp_1yr + care_home + hhf + adm_type + comorb_fct + hdb + 
            hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
            hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae + sbp + 
            bmi + smoking + nyha, data = x, ties = "breslow"
        )
      )
    } else if (.by %in% c("hfref", "hfpef", "unspecified", "specified")) {
      res_ls <- map(
        ls, \(x) coxph(
          Surv(ach_emerg_tte_1yr, ach_emerg_ep_1yr) ~ age + sex + ethnic + imd_fct +
            hcv19 + prior_hosp_1yr + care_home + hhf + adm_type + comorb_fct + hdb + 
            hhtn + hckd + hobes + hihd + hpad + haf + hvalvhd + hstk + hautoimmune + 
            hcanc + hdem + hdep + host + hthy + hasthm + hcopd + hlivd + hanae + sbp + 
            bmi + smoking + nyha, data = x, ties = "breslow"
        )
      )
    }
  }
  
  pafs <- map2(
    res_ls, ls, 
    \(x, y) AFcoxph(x, data = y, exposure = .comorb, times = "365.25") |>
      extract_paf(.comorb)
  ) |>
    bind_rows() |>
    mutate(fold = row_number(), .before = 1) |>
    mutate(
      group = .by,
      # Calculate weights
      weights = 1 / paf_se^2,
    )
  
  # Calculate weighted mean
  weighted_mean <- sum(pafs$paf * pafs$weights) / sum(pafs$weights)
  
  # Calculate the effective variance of the weighted mean
  effective_variance <- 1 / sum(pafs$weights)
  
  # Standard error of the weighted mean
  se_weighted_mean <- sqrt(effective_variance)
  
  # Assuming a normal distribution, calculate 95% confidence interval
  # Critical value for the normal distribution at 95% confidence level (z-value)
  z <- qnorm(0.975)
  
  # Confidence interval
  ci_lower <- weighted_mean - z * se_weighted_mean
  ci_upper <- weighted_mean + z * se_weighted_mean
  
  # Add results
  pafs <- pafs |>
    mutate(mean  = weighted_mean, weighted_lci  = ci_lower, weighted_uci = ci_upper)
  
  return(pafs)
}

# paf_ckd_all <- get_paf_mi(mi, .endpoint = "mortality", .comorb = "hckd")
# paf_ckd_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "mortality", .comorb = "hckd")
# paf_ckd_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "mortality", .comorb = "hckd")
# paf_ckd_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "mortality", .comorb = "hckd")
# paf_ckd_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "mortality", .comorb = "hckd")
# paf_ckd_death <- 
#   bind_rows(paf_ckd_all, paf_ckd_ref, paf_ckd_pef, paf_ckd_spe, paf_ckd_uns)
# 
# paf_ckd_all <- get_paf_mi(mi, .endpoint = "hospitalisation", .comorb = "hckd")
# paf_ckd_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "hospitalisation", .comorb = "hckd")
# paf_ckd_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "hospitalisation", .comorb = "hckd")
# paf_ckd_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "hospitalisation", .comorb = "hckd")
# paf_ckd_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "hospitalisation", .comorb = "hckd")
# paf_ckd_hosp <- 
#   bind_rows(paf_ckd_all, paf_ckd_ref, paf_ckd_pef, paf_ckd_spe, paf_ckd_uns)


# Get PAF for AF ----------------------------------------------------------

paf_af_all <- get_paf_mi(mi, .endpoint = "mortality", .comorb = "haf")
paf_af_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "mortality", .comorb = "haf")
paf_af_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "mortality", .comorb = "haf")
paf_af_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "mortality", .comorb = "haf")
paf_af_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "mortality", .comorb = "haf")
paf_af_death <- 
  bind_rows(paf_af_all, paf_af_ref, paf_af_pef, paf_af_spe, paf_af_uns) |>
  mutate(endpoint = "mortality")

paf_af_all <- get_paf_mi(mi, .endpoint = "hospitalisation", .comorb = "haf")
paf_af_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "hospitalisation", .comorb = "haf")
paf_af_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "hospitalisation", .comorb = "haf")
paf_af_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "hospitalisation", .comorb = "haf")
paf_af_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "hospitalisation", .comorb = "haf")
paf_af_hosp <- 
  bind_rows(paf_af_all, paf_af_ref, paf_af_pef, paf_af_spe, paf_af_uns) |>
  mutate(endpoint = "hospitalisation")

paf_af <- bind_rows(paf_af_death, paf_af_hosp)

write_csv(paf_af, here::here("figs", "comorbidities", "13_paf_af.csv"))


# Get PAF for dementia ----------------------------------------------------------

paf_dem_all <- get_paf_mi(mi, .endpoint = "mortality", .comorb = "hdem")
paf_dem_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "mortality", .comorb = "hdem")
paf_dem_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "mortality", .comorb = "hdem")
paf_dem_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "mortality", .comorb = "hdem")
paf_dem_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "mortality", .comorb = "hdem")
paf_dem_death <- 
  bind_rows(paf_dem_all, paf_dem_ref, paf_dem_pef, paf_dem_spe, paf_dem_uns) |>
  mutate(endpoint = "mortality")

paf_dem_all <- get_paf_mi(mi, .endpoint = "hospitalisation", .comorb = "hdem")
paf_dem_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "hospitalisation", .comorb = "hdem")
paf_dem_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "hospitalisation", .comorb = "hdem")
paf_dem_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "hospitalisation", .comorb = "hdem")
paf_dem_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "hospitalisation", .comorb = "hdem")
paf_dem_hosp <- 
  bind_rows(paf_dem_all, paf_dem_ref, paf_dem_pef, paf_dem_spe, paf_dem_uns) |>
  mutate(endpoint = "hospitalisation")

paf_dem <- bind_rows(paf_dem_death, paf_dem_hosp)

write_csv(paf_dem, here::here("figs", "comorbidities", "14_paf_dem.csv"))


# Get PAF for IHD ---------------------------------------------------------

paf_ihd_all <- get_paf_mi(mi, .endpoint = "mortality", .comorb = "hihd")
paf_ihd_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "mortality", .comorb = "hihd")
paf_ihd_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "mortality", .comorb = "hihd")
paf_ihd_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "mortality", .comorb = "hihd")
paf_ihd_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "mortality", .comorb = "hihd")
paf_ihd_death <- 
  bind_rows(paf_ihd_all, paf_ihd_ref, paf_ihd_pef, paf_ihd_spe, paf_ihd_uns) |>
  mutate(endpoint = "mortality")

paf_ihd_all <- get_paf_mi(mi, .endpoint = "hospitalisation", .comorb = "hihd")
paf_ihd_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "hospitalisation", .comorb = "hihd")
paf_ihd_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "hospitalisation", .comorb = "hihd")
paf_ihd_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "hospitalisation", .comorb = "hihd")
paf_ihd_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "hospitalisation", .comorb = "hihd")
paf_ihd_hosp <- 
  bind_rows(paf_ihd_all, paf_ihd_ref, paf_ihd_pef, paf_ihd_spe, paf_ihd_uns) |>
  mutate(endpoint = "hospitalisation")

paf_ihd <- bind_rows(paf_ihd_death, paf_ihd_hosp)


# # Get PAF for stroke ---------------------------------------------------------

paf_stk_all <- get_paf_mi(mi, .endpoint = "mortality", .comorb = "hstk")
paf_stk_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "mortality", .comorb = "hstk")
paf_stk_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "mortality", .comorb = "hstk")
paf_stk_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "mortality", .comorb = "hstk")
paf_stk_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "mortality", .comorb = "hstk")
paf_stk_death <-
  bind_rows(paf_stk_all, paf_stk_ref, paf_stk_pef, paf_stk_spe, paf_stk_uns) |>
  mutate(endpoint = "mortality")

paf_stk_all <- get_paf_mi(mi, .endpoint = "hospitalisation", .comorb = "hstk")
paf_stk_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "hospitalisation", .comorb = "hstk")
paf_stk_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "hospitalisation", .comorb = "hstk")
paf_stk_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "hospitalisation", .comorb = "hstk")
paf_stk_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "hospitalisation", .comorb = "hstk")
paf_stk_hosp <-
  bind_rows(paf_stk_all, paf_stk_ref, paf_stk_pef, paf_stk_spe, paf_stk_uns) |>
  mutate(endpoint = "hospitalisation")

paf_stk <- bind_rows(paf_stk_death, paf_stk_hosp)


# Get PAF for COPD ---------------------------------------------------------

paf_copd_all <- get_paf_mi(mi, .endpoint = "mortality", .comorb = "hcopd")
paf_copd_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "mortality", .comorb = "hcopd")
paf_copd_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "mortality", .comorb = "hcopd")
paf_copd_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "mortality", .comorb = "hcopd")
paf_copd_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "mortality", .comorb = "hcopd")
paf_copd_death <-
  bind_rows(paf_copd_all, paf_copd_ref, paf_copd_pef, paf_copd_spe, paf_copd_uns) |>
  mutate(endpoint = "mortality")

paf_copd_all <- get_paf_mi(mi, .endpoint = "hospitalisation", .comorb = "hcopd")
paf_copd_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "hospitalisation", .comorb = "hcopd")
paf_copd_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "hospitalisation", .comorb = "hcopd")
paf_copd_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "hospitalisation", .comorb = "hcopd")
paf_copd_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "hospitalisation", .comorb = "hcopd")
paf_copd_hosp <-
  bind_rows(paf_copd_all, paf_copd_ref, paf_copd_pef, paf_copd_spe, paf_copd_uns) |>
  mutate(endpoint = "hospitalisation")

paf_copd <- bind_rows(paf_copd_death, paf_copd_hosp)


# Get PAF for cancer ---------------------------------------------------------

paf_canc_all <- get_paf_mi(mi, .endpoint = "mortality", .comorb = "hcanc")
paf_canc_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "mortality", .comorb = "hcanc")
paf_canc_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "mortality", .comorb = "hcanc")
paf_canc_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "mortality", .comorb = "hcanc")
paf_canc_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "mortality", .comorb = "hcanc")
paf_canc_death <-
  bind_rows(paf_canc_all, paf_canc_ref, paf_canc_pef, paf_canc_spe, paf_canc_uns) |>
  mutate(endpoint = "mortality")

paf_canc_all <- get_paf_mi(mi, .endpoint = "hospitalisation", .comorb = "hcanc")
paf_canc_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "hospitalisation", .comorb = "hcanc")
paf_canc_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "hospitalisation", .comorb = "hcanc")
paf_canc_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "hospitalisation", .comorb = "hcanc")
paf_canc_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "hospitalisation", .comorb = "hcanc")
paf_canc_hosp <-
  bind_rows(paf_canc_all, paf_canc_ref, paf_canc_pef, paf_canc_spe, paf_canc_uns) |>
  mutate(endpoint = "hospitalisation")

paf_canc <- bind_rows(paf_canc_death, paf_canc_hosp)


# Get PAF for dementia ---------------------------------------------------------

paf_dem_all <- get_paf_mi(mi, .endpoint = "mortality", .comorb = "hdem")
paf_dem_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "mortality", .comorb = "hdem")
paf_dem_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "mortality", .comorb = "hdem")
paf_dem_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "mortality", .comorb = "hdem")
paf_dem_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "mortality", .comorb = "hdem")
paf_dem_death <-
  bind_rows(paf_dem_all, paf_dem_ref, paf_dem_pef, paf_dem_spe, paf_dem_uns) |>
  mutate(endpoint = "mortality")

paf_dem_all <- get_paf_mi(mi, .endpoint = "hospitalisation", .comorb = "hdem")
paf_dem_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "hospitalisation", .comorb = "hdem")
paf_dem_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "hospitalisation", .comorb = "hdem")
paf_dem_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "hospitalisation", .comorb = "hdem")
paf_dem_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "hospitalisation", .comorb = "hdem")
paf_dem_hosp <-
  bind_rows(paf_dem_all, paf_dem_ref, paf_dem_pef, paf_dem_spe, paf_dem_uns) |>
  mutate(endpoint = "hospitalisation")

paf_dem <- bind_rows(paf_dem_death, paf_dem_hosp)


# Get PAF for VHD ---------------------------------------------------------

paf_valvhd_all <- get_paf_mi(mi, .endpoint = "mortality", .comorb = "hvalvhd")
paf_valvhd_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "mortality", .comorb = "hvalvhd")
paf_valvhd_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "mortality", .comorb = "hvalvhd")
paf_valvhd_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "mortality", .comorb = "hvalvhd")
paf_valvhd_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "mortality", .comorb = "hvalvhd")
paf_valvhd_death <-
  bind_rows(paf_valvhd_all, paf_valvhd_ref, paf_valvhd_pef, paf_valvhd_spe, paf_valvhd_uns) |>
  mutate(endpoint = "mortality")

paf_valvhd_all <- get_paf_mi(mi, .endpoint = "hospitalisation", .comorb = "hvalvhd")
paf_valvhd_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "hospitalisation", .comorb = "hvalvhd")
paf_valvhd_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "hospitalisation", .comorb = "hvalvhd")
paf_valvhd_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "hospitalisation", .comorb = "hvalvhd")
paf_valvhd_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "hospitalisation", .comorb = "hvalvhd")
paf_valvhd_hosp <-
  bind_rows(paf_valvhd_all, paf_valvhd_ref, paf_valvhd_pef, paf_valvhd_spe, paf_valvhd_uns) |>
  mutate(endpoint = "hospitalisation")

paf_valvhd <- bind_rows(paf_valvhd_death, paf_valvhd_hosp)


# Get PAF for livd ---------------------------------------------------------

paf_livd_all <- get_paf_mi(mi, .endpoint = "mortality", .comorb = "hlivd")
paf_livd_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "mortality", .comorb = "hlivd")
paf_livd_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "mortality", .comorb = "hlivd")
paf_livd_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "mortality", .comorb = "hlivd")
paf_livd_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "mortality", .comorb = "hlivd")
paf_livd_death <-
  bind_rows(paf_livd_all, paf_livd_ref, paf_livd_pef, paf_livd_spe, paf_livd_uns) |>
  mutate(endpoint = "mortality")

paf_livd_all <- get_paf_mi(mi, .endpoint = "hospitalisation", .comorb = "hlivd")
paf_livd_ref <- get_paf_mi(mi, .by = "hfref", .endpoint = "hospitalisation", .comorb = "hlivd")
paf_livd_pef <- get_paf_mi(mi, .by = "hfpef", .endpoint = "hospitalisation", .comorb = "hlivd")
paf_livd_spe <- get_paf_mi(mi, .by = "specified", .endpoint = "hospitalisation", .comorb = "hlivd")
paf_livd_uns <- get_paf_mi(mi, .by = "unspecified", .endpoint = "hospitalisation", .comorb = "hlivd")
paf_livd_hosp <-
  bind_rows(paf_livd_all, paf_livd_ref, paf_livd_pef, paf_livd_spe, paf_livd_uns) |>
  mutate(endpoint = "hospitalisation")

paf_livd <- bind_rows(paf_livd_death, paf_livd_hosp)


# Combine -----------------------------------------------------------------

# paf <- bind_rows(
#   paf_ihd, paf_stk, paf_copd, paf_canc, paf_dem, paf_vhd, paf_livd
# )


# Write data --------------------------------------------------------------

write_csv(paf_ckd_death, here::here("figs", "comorbidities", "04_paf_ckd_mortality.csv"))
write_csv(paf_ckd_hosp, here::here("figs", "comorbidities", "05_paf_ckd_hospitalisation.csv"))

write_csv(paf_ihd, here::here("figs", "comorbidities", "06_paf_ihd.csv"))
write_csv(paf_stk, here::here("figs", "comorbidities", "07_paf_stk.csv"))
write_csv(paf_copd, here::here("figs", "comorbidities", "08_paf_copd.csv"))
write_csv(paf_canc, here::here("figs", "comorbidities", "09_paf_canc.csv"))
write_csv(paf_dem, here::here("figs", "comorbidities", "10_paf_dem.csv"))
write_csv(paf_valvhd, here::here("figs", "comorbidities", "11_paf_valvhd.csv"))
write_csv(paf_livd, here::here("figs", "comorbidities", "12_paf_livd.csv"))

# write_csv(paf, here::here("figs", "comorbidities", "06_paf_all_other.csv"))



