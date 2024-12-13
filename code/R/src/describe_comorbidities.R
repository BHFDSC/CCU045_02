describe_comorbidities <- function(.data, .by, .grp) {
  if (.by == "comorb_fct") {
    .data <- .data |> 
      mutate(
        comorb_fct = fct_relevel(comorb_fct, "≥5"),
        across(
          c(hdb, hckd, hobes, hlivd, hcanc, hdem, hdep, host, hthy, hasthm, 
            hcopd, hautoimmune, hhtn, haf, harryth, hvalvhd, hihd, hpad, hstk, 
            hanae),
          \(x) case_when(
            x == 0 ~ paste0("no_", deparse(substitute(x))),
            x == 1 ~ paste0("yes_", deparse(substitute(x))),
            .default = NA
          )
        )
      )
  }
  # Separate into groups
  sep <- split(.data, .data[[.by]])
  
  # Summarise non-normally-distributed continuous variables
  tbl1 <- 
    map2(
      sep, .grp, 
      \(x, y) summarise_continuous(
        x, 
        c(prior_hosp, prior_hosp_1yr, prior_hosp_peryr, prior_gp, 
          prior_gp_1yr, prior_gp_peryr), 
        .header = y, .normally_distributed = FALSE, .decimals = 1
      ) 
    ) |>
    reduce(left_join, by = join_by(Characteristic)) # |>
  # mutate(across(everything(), \(x) str_remove_all(x, "[.]")))
  
  tbl1_ttl <- 
    summarise_continuous(
      .data, 
      c(prior_hosp, prior_hosp_1yr, prior_hosp_peryr, prior_gp, 
        prior_gp_1yr, prior_gp_peryr),
      .header = "All patients", .normally_distributed = FALSE, .decimals = 1
    ) # |>
  # mutate(across(everything(), \(x) str_remove_all(x, "[.]")))
  
  if (.by != "comorb_fct") {
    # Summarise categorical variables
    tbl2 <- 
      map2(
        sep, .grp, 
        \(x, y) summarise_categorical(
          x, c(comorb_fct, hdb, hckd, hobes, hlivd, hcanc, hdem, hdep, host, hthy, 
               hasthm, hcopd, hautoimmune, hhtn, haf, harryth, hvalvhd, hihd, 
               hpad, hstk, hanae), 
          .header = y
        ) |>
          mutate(row = as.character(row_number()))
      ) |> 
      reduce(left_join, by = join_by(Characteristic, row)) |>
      filter(!str_detect(Characteristic, "0")) |>
      mutate(
        across(
          everything(),
          \(x) if_else(str_detect(x, "^5 "), "REDACTED", x)
        ),
        across(
          everything(),
          \(x) if_else(x == "" & Characteristic != "comorb_fct", NA, x)
        )
      )
    
    tbl2_ttl <- 
      summarise_categorical(
        .data, 
        c(comorb_fct, hdb, hckd, hobes, hlivd, hcanc, hdem, hdep, host, hthy, 
          hasthm, hcopd, hautoimmune, hhtn, haf, harryth, hvalvhd, hihd, hpad,
          hstk, hanae), 
        .header = "All patients"
      ) |>
      mutate(row = as.character(row_number())) |>
      filter(!str_detect(Characteristic, "0")) |>
      mutate(
        across(
          everything(),
          \(x) if_else(str_detect(x, "^5 "), "REDACTED", x)
        ),
        across(
          everything(),
          \(x) if_else(x == "" & Characteristic != "comorb_fct", NA, x)
        )
      )
  } else {
    # Summarise categorical variables
    tbl2 <- 
      map2(
        sep, .grp, 
        \(x, y) summarise_categorical(
          x, c(hdb, hckd, hobes, hlivd, hcanc, hdem, hdep, host, hthy, 
               hasthm, hcopd, hautoimmune, hhtn, haf, harryth, hvalvhd, hihd, 
               hpad, hstk, hanae), 
          .header = y
        )
      ) |> 
      reduce(left_join, by = join_by(Characteristic)) |>
      filter(!(str_detect(Characteristic, "  No "))) |>
      mutate(
        across(
          everything(),
          \(x) if_else(is.na(x), "0 (0.0%)", x)
        ),
        across(
          everything(),
          \(x) if_else(str_detect(x, "^5 "), "REDACTED", x)
        ),
        across(
          everything(),
          \(x) if_else(x == "", NA, x)
        )
      )
    
    tbl2_ttl <- 
      summarise_categorical(
        .data, 
        c(hdb, hckd, hobes, hlivd, hcanc, hdem, hdep, host, hthy, hasthm, hcopd, 
          hautoimmune, hhtn, haf, harryth, hvalvhd, hihd, hpad, hstk, hanae), 
        .header = "All patients"
      ) |>
      filter(!str_detect(Characteristic, "  No ")) |>
      mutate(
        across(
          everything(),
          \(x) if_else(is.na(x), "0 (0.0%)", x)
        ),
        across(
          everything(),
          \(x) if_else(x == "", NA, x)
        )
      )
  }
  
  tbl1 <- tbl1 |> left_join(tbl1_ttl, by = join_by(Characteristic))
  if (.by != "comorb_fct") {
    tbl2 <- tbl2 |> 
      left_join(tbl2_ttl, by = join_by(Characteristic, row)) |>
      select(-row)
    tbl <- bind_rows(tbl1, tbl2) |>
      fill(names(tbl1), .direction = "up") |> 
      filter(!(str_detect(Characteristic, "1$") & row_number() > 12))
  } else {
    tbl2 <- tbl2 |> 
      left_join(tbl2_ttl, by = join_by(Characteristic))
    tbl <- bind_rows(tbl1, tbl2) |>
      fill(names(tbl1), .direction = "up") |> 
      filter(!str_detect(Characteristic, "  Yes "))
  }
  
  # Combine
  tbl <- tbl |>
    left_join(comorb_names, by = join_by(Characteristic)) |>
    mutate(
      Characteristic = case_when(
        !is.na(name) ~ name,
        .default = Characteristic
      )
    ) |>
    select(-name)
  
  # Function to select rows
  select_row <- function(x, y) {
    filter(x, str_detect(Characteristic, regex(y, ignore_case = TRUE)))
  }
  
  # Rearrange table rows
  hosp <- tbl |> 
    select_row("^prior hosp") |>
    mutate(
      Characteristic = str_remove_all(Characteristic, "Prior hospitalisations, "),
      Characteristic = str_remove_all(Characteristic, ", median [(]IQR[)]"),
      Characteristic = str_to_sentence(Characteristic),
      Characteristic = paste0("  ", Characteristic)
    ) |>
    add_row(Characteristic = "Prior hospitalistions, median (IQR)", .before = 1)
  gp <- tbl |> 
    select_row("^prior gp") |>
    mutate(
      Characteristic = str_remove_all(Characteristic, "Prior GP visits, "),
      Characteristic = str_remove_all(Characteristic, ", median [(]IQR[)]"),
      Characteristic = str_to_sentence(Characteristic),
      Characteristic = paste0("  ", Characteristic)
    ) |>
    add_row(Characteristic = "Prior GP visits, median (IQR)", .before = 1)
  comorb <- tbl |> select_row("Number of comor|zero|[0-5]$") |>
    mutate(Characteristic = str_replace_all(Characteristic, "Zero", "0"))
  ind_co <- tbl |> 
    filter(!str_detect(Characteristic, regex("^prior|^number|zero|[0-5]$", ignore_case = TRUE))) |>
    add_row(Characteristic = "Specific comorbidities, no. (%)", .before = 1)
  
  if (.by != "comorb_fct") {
    tbl_final <- bind_rows(hosp, gp, comorb, ind_co) |>
      mutate(across(everything(), \(x) if_else(is.na(x), "", x)))
  } else {
    tbl_final <- bind_rows(hosp, gp, ind_co) |>
      relocate(matches("^≥5"), .after = 7) |>
      mutate(across(everything(), \(x) if_else(is.na(x), "", x)))
  }
  
  if (.by == "hf_subtype") {
    tbl_final <- tbl_final |>
      relocate(matches("ref"), .after = 1) |>
      rename_with(
        \(x) case_when(
          str_detect(x, "ref") ~ str_replace(x, "Hfref", "Heart failure with reduced ejection fraction"),
          str_detect(x, "pef") ~ str_replace(x, "Hfpef", "Heart failure with preserved ejection fraction"),
          str_detect(x, "Unk") ~ str_replace(x, "Unknown", "Unknown subclass"),
          .default = x
        )
      )
    return(tbl_final)
  } else if (.by == "period") {
    tbl_final <- tbl_final |>
      rename_with(
        \(x) case_when(
          str_detect(x, "^1 ") ~ str_replace(x, "1", "2019"),
          str_detect(x, "^2 ") ~ str_replace(x, "2", "2020"),
          str_detect(x, "^3 ") ~ str_replace(x, "3", "2021"),
          str_detect(x, "^4 ") ~ str_replace(x, "4", "2022"),
          str_detect(x, "^5 ") ~ str_replace(x, "5", "2023"),
          .default = x
        )
      )
    return(tbl_final)
  } else if (.by == "new_hf") {
    tbl_final <- tbl_final |>
      filter(!str_detect(Characteristic, "New onset")) |>
      relocate(matches("^1 "), .after = 1) |>
      rename_with(
        \(x) case_when(
          str_detect(x, "^0 ") ~ str_replace(x, "0", "Pre-existing heart failure"),
          str_detect(x, "^1 ") ~ str_replace(x, "1", "New onset heart failure"),
          .default = x
        )
      )
    return(tbl_final)
  } else if (.by == "new_hf_dth") {
    tbl_final <- tbl_final |>
      relocate(matches("^1 "), .after = 1) |>
      rename_with(
        \(x) case_when(
          str_detect(x, "^0 ") ~ str_replace(x, "0", "Did not die during index heart failure admission"),
          str_detect(x, "^1 ") ~ str_replace(x, "1", "Died during index heart failure admission"),
          .default = x
        )
      )
    return(tbl_final)
  } else {
    return(tbl_final)
  }
}