#' Calculate eGFR using the CKD-EPI 2021 race-free equation
#'
#' @details 
#' Source: Table S3 of https://www.kidney.org/content/national-kidney-foundation-laboratory-engagement-working-group-recommendations-implementing#table-s3-equations-to-calculate-egfrcr-creatinine-si-units


calculate_egfr <- function(.data,
                           .scr,
                           .age,
                           .sex,
                           .gfr_to,
                           .formula = "ckd-epi-2021") {
  
  # Wrapper for `str_detect({{ .var }}, regex(.str, ignore_case = TRUE))`
  like <- function(.var, .str) {
    stringr::str_detect({{ .var }}, stringr::regex(.str, ignore_case = TRUE))
  }
    
  if (.formula == "ckd-epi-2021") {
    
    apply_ckd_epi_2021 <- function(.grp, .scr, .age) {
      if (.grp == "Females Scr <= 61.9") {
        
        142 * (.scr / 61.88)^-0.241 * 0.9938^.age * 1.012
      
      } else if (.grp == "Females Scr > 61.9") {
        
        142 * (.scr / 61.88)^-1.200 * 0.9938^.age * 1.012
        
      } else if (.grp == "Males Scr <= 79.6") {
        
        142 * (.scr / 79.56)^-0.302 * 0.9938^.age
        
      } else if (.grp == "Males Scr > 79.6") {
       
        142 * (.scr / 79.56)^-1.200 * 0.9938^.age
        
      }
    }
    
    .data <- .data %>%
      mutate(
        adult = case_when(
          {{ .age }} >= 18 ~ 1, 
          {{ .age }} < 18  ~ 0,
          TRUE ~ NA_real_
        ),
        female = case_when(
          like({{ .sex }}, "^f") ~ 1, 
          like({{ .sex }}, "^m") ~ 0,
          TRUE ~ NA_real_
        ),
        scr_grp = case_when(
          female == 1 & {{ .scr }} <= 61.9 ~ "fl",
          female == 1 & {{ .scr }} > 61.9  ~ "fh",
          female == 0 & {{ .scr }} <= 79.6 ~ "ml",
          female == 0 & {{ .scr }} > 79.6  ~ "mh",
          TRUE ~ NA_character_
        ),
        !!.gfr_to := case_when(
          scr_grp == "fl" ~ apply_ckd_epi_2021(
            .grp = "Females Scr <= 61.9", .scr = {{ .scr }}, .age = {{ .age }}
          ),
          scr_grp == "fh" ~ apply_ckd_epi_2021(
            .grp = "Females Scr > 61.9", .scr = {{ .scr }}, .age = {{ .age }}
          ),
          scr_grp == "ml" ~ apply_ckd_epi_2021(
            .grp = "Males Scr <= 79.6", .scr = {{ .scr }}, .age = {{ .age }}
          ),
          scr_grp == "mh" ~ apply_ckd_epi_2021(
            .grp = "Males Scr > 79.6", .scr = {{ .scr }}, .age = {{ .age }}
          ),
          TRUE ~ NA_real_
        )
      ) %>%
      select(-c(adult, female, scr_grp))
  }
  return(.data)
}