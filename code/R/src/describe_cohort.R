describe_cohort <- function(.data, .by, .grp) {
  if (.by != "hf_subtype") {
    .data <- .data |> mutate(hf_subtype = fct_relevel(hf_subtype, "hfref")) 
  }
  
  if (.by == "comorb_fct") {
    .data <- .data |> 
      mutate(comorb_fct = fct_relevel(comorb_fct, "≥5"))
  }
  # Separate into groups
  sep <- split(.data, .data[[.by]])
  grp <- c("HFpEF", "HFrEF", "Unknown")
  
  # Summarise normally-distributed continuous variables
  tbl1 <- 
    map2(
      sep, .grp, 
      \(x, y) summarise_continuous(
        x, c(age, bmi, sbp, egfr, k), .header = y
      ) 
    ) |>
    reduce(left_join, by = join_by(Characteristic))
  
  tbl1_ttl <- summarise_continuous(
    .data, c(age, bmi, sbp, egfr, k), .header = "All patients"
  )
  
  # Summarise categorical variables
  tbl2 <- 
    map2(
      sep, .grp, 
      \(x, y) summarise_categorical(
        x, c(bmi_na, sbp_na, egfr_na, k_na, bnp_na, nt_pro_bnp_na, hcv19, 
             care_home, sex, ethnic, smok_fct, imd_fct, nyha_fct, new_hf,
             BETABLOCK_6mo_after, ACEI_6mo_after, ARB_6mo_after, ARNI_6mo_after,
             RAS_6mo_after, MRA_6mo_after, DIURETIC_6mo_after, SGLT2I_6mo_after),
        # .decimals = 0,
        .header = y
      ) |>
        mutate(row = row_number())
    ) |> 
    reduce(left_join, by = join_by(Characteristic, row)) |>
    filter(!str_detect(Characteristic, "0$")) |>
    mutate(
      across(
        everything(),
        \(x) if_else(
          x == "" & str_detect(Characteristic, "_na$|^new_|^hcv|^care|6mo"),
          NA, x
        )
      )
    )
  
  tbl2_ttl <- summarise_categorical(
    .data, 
    c(bmi_na, sbp_na, egfr_na, k_na, bnp_na, nt_pro_bnp_na, hcv19, 
      care_home, sex, ethnic, smok_fct, imd_fct, nyha_fct, new_hf,
      BETABLOCK_6mo_after, ACEI_6mo_after, ARB_6mo_after, ARNI_6mo_after,
      RAS_6mo_after, MRA_6mo_after, DIURETIC_6mo_after, SGLT2I_6mo_after),
    # .decimals = 0,
    .header = "All patients"
  ) |>
    mutate(row = row_number()) |>
    filter(!str_detect(Characteristic, "0$")) |>
    mutate(
      across(
        everything(),
        \(x) if_else(
          x == "" & str_detect(Characteristic, "_na$|^new_|^hcv|^care|6mo"),
          NA, x
        )
      )
    )
  
  # Summarise non-normally-distributed continuous variables
  tbl3 <- 
    map2(
      sep, .grp, 
      \(x, y) summarise_continuous(
        x, c(tar, bnp, nt_pro_bnp), 
        .header = y, .normally_distributed = FALSE, .decimals = 1
      ) 
    ) |>
    reduce(left_join, by = join_by(Characteristic))
  
  tbl3_ttl <- 
    summarise_continuous(
      .data, c(tar, bnp, nt_pro_bnp),
      .header = "All patients", .normally_distributed = FALSE, .decimals = 1
    )
  
  if (.by != "hf_subtype") {
    hfs <- 
      map2(
        sep, .grp, 
        \(x, y) summarise_categorical(x, hf_subtype, .header = y)
      ) |> 
      reduce(left_join, by = join_by(Characteristic))
    
    hfs_ttl <- summarise_categorical(
      .data, hf_subtype, .header = "All patients"
    )
    hfs <- hfs |> 
      left_join(hfs_ttl, by = join_by(Characteristic)) |>
      mutate(
        across(
          everything(),
          \(x) if_else(str_detect(x, "^5 "), "REDACTED", x)
        )
      )
  }
  
  tbl1 <- tbl1 |> left_join(tbl1_ttl, by = join_by(Characteristic))
  tbl2 <- tbl2 |> 
    left_join(tbl2_ttl, by = join_by(Characteristic, row)) |>
    select(-row) |>
    mutate(
      across(
        everything(),
        \(x) if_else(str_detect(x, "^5 "), "REDACTED", x)
      )
    )
  tbl3 <- tbl3 |> left_join(tbl3_ttl, by = join_by(Characteristic))
  
  if (.by != "hf_subtype") {
    tbl <- bind_rows(tbl1, tbl2, tbl3, hfs)
  } else {
    tbl <- bind_rows(tbl1, tbl2, tbl3)
  }
  # Combine
  tbl <- tbl |>
    fill(names(tbl1), .direction = "up") |> 
    filter(!(str_detect(Characteristic, "1$") & row_number() < 33)) |>
    left_join(char_names, by = join_by(Characteristic)) |>
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
  
  # Function to rename missing
  rename_na <- function(x) {
    mutate(
      x, Characteristic = if_else(
        str_detect(Characteristic, "Missing"), "  Missing", Characteristic
      )
    )
  }
  
  # Rearrange table rows
  age <- tbl |> select_row("^age")
  sex <- tbl |> select_row("^sex|male$")
  eth <- tbl |> 
    select_row("^ethnic|white$|black$|asian$|mixed$|other$|missing ethnic") |>
    rename_na()
  imd <- tbl |> 
    select_row("^socioeconomic|least dep|  2$|  3$|  4$|most dep|missing imd") |>
    rename_na()
  smok <- tbl |> 
    select_row("^smok|current|former|never|missing smok") |>
    rename_na()
  nyha <- 
    bind_rows(
      tbl |> select_row("^NYHA"),
      tbl |> select_row("  I$|  II$|  III$|  IV$") |> mutate(Characteristic = toupper(Characteristic)),
      tbl |> select_row("missing nyha")
    ) |>
    rename_na()
  sub <- tbl |>
    select_row("subtype|hfref|hfpef|unspecified") |>
    mutate(
      Characteristic = case_when(
        str_detect(Characteristic, "subtype") ~ "Heart failure subclass, no. (%)",
        str_detect(Characteristic, "Hfref") ~ "  Reduced ejection fraction",
        str_detect(Characteristic, "Hfpef") ~ "  Preserved ejection fraction",
        str_detect(Characteristic, "Unspec") ~ "  Unknown"
      )
    )
  sbp <-
    tbl |> select_row("sbp|systolic") |>
    mutate(
      Characteristic = case_when(
        str_detect(Characteristic, "Systolic") ~ "  Mean (SD)",
        str_detect(Characteristic, "Missing") ~ "  Missing, no. (%)"
      )
    ) |>
    add_row(Characteristic = "Systolic blood pressure, mm Hg", .before = 1)
  bmi <-
    tbl |> select_row("bmi|body-mass") |>
    mutate(
      Characteristic = case_when(
        str_detect(Characteristic, "Body-mass") ~ "  Mean (SD)",
        str_detect(Characteristic, "Missing") ~ "  Missing, no. (%)"
      )
    ) |>
    add_row(Characteristic = "Body-mass index, kg/m2", .before = 1)
  gfr <-
    tbl |> select_row("egfr|^estimated g") |>
    mutate(
      Characteristic = case_when(
        str_detect(Characteristic, "Estimated GFR") ~ "  Mean (SD)",
        str_detect(Characteristic, "Missing") ~ "  Missing, no. (%)"
      )
    ) |>
    add_row(Characteristic = "Estimated GFR, mL/min/1.73m2", .before = 1)
  k <-
    tbl |> select_row("Missing pot|^Serum potassium") |>
    mutate(
      Characteristic = case_when(
        str_detect(Characteristic, "Serum potassium") ~ "  Mean (SD)",
        str_detect(Characteristic, "Missing") ~ "  Missing, no. (%)"
      )
    ) |>
    add_row(Characteristic = "Serum potassium, mmol/L", .before = 1)
  bnp <-
    tbl |> select_row("Missing BNP|^B-type") |>
    mutate(
      Characteristic = case_when(
        str_detect(Characteristic, "B-type") ~ "  Median (IQR)",
        str_detect(Characteristic, "Missing") ~ "  Missing, no. (%)"
      )
    ) |>
    arrange(Characteristic) |>
    add_row(Characteristic = "B-type natriuretic peptide, pg/mL", .before = 1)
  ntbnp <-
    tbl |> select_row("Missing NT|^N-terminal pr") |>
    mutate(
      Characteristic = case_when(
        str_detect(Characteristic, "N-terminal pr") ~ "  Median (IQR)",
        str_detect(Characteristic, "Missing") ~ "  Missing, no. (%)"
      )
    ) |>
    arrange(Characteristic) |>
    add_row(Characteristic = "N-terminal pro B-type natriuretic peptide, pg/mL", .before = 1)
  new_hf <- tbl |> select_row("^New onset heart")
  cv19 <- tbl |> select_row("^Prior COVID")
  ch <- tbl |> select_row("care home")
  tr <- tbl |> select_row("^time at risk")
  
  med <- tibble(Characteristic = "Medication use at discharge")
  bb <- tbl |> select_row("^  beta blocker")
  ace <- tbl |> select_row("^  ACE inhibitor$")
  arb <- tbl |> select_row("^  ARB")
  arni <- tbl |> select_row("^  ARNI")
  ras <- tbl |> select_row("^  ACE inhibitor[/]ARB[/]ARNI")
  mra <- tbl |> select_row("^  MRA")
  diu <- tbl |> select_row("^  Loop Diuretic")
  sgl <- tbl |> select_row("^  SGLT2")
  
  if (.by != "hf_subtype") {
    tbl_final <- bind_rows(
      age, sex, eth, imd, smok, nyha, sub, bmi, sbp, gfr, k, bnp, ntbnp, new_hf,
      cv19, ch, tr, med, bb, ace, arb, arni, ras, mra, diu, sgl
    ) |>
      mutate(across(everything(), \(x) if_else(is.na(x), "", x)))
  } else {
    tbl_final <- bind_rows(
      age, sex, eth, imd, smok, nyha, bmi, sbp, gfr, k, bnp, ntbnp, new_hf,
      cv19, ch, tr, med, bb, ace, arb, arni, ras, mra, diu, sgl
    ) |>
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
  } else if (.by == "comorb_fct") {
    tbl_final <- tbl_final |>
      relocate(matches("^≥5"), .after = 7)
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