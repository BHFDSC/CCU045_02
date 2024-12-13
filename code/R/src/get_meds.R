get_meds <- function(.data, .group_by = NULL) {
  perc <- .data |> 
    summarise(
      n = n_distinct(subject_id), 
      sglt2 = sum(SGLT2I_6mo_after), 
      bb = sum(BETABLOCK_6mo_after), 
      mra = sum(MRA_6mo_after), 
      arb = sum(ARB_6mo_after),
      ace = sum(ACEI_6mo_after),
      arni = sum(ARNI_6mo_after),
      ras = sum(RAS_6mo_after),
      diur = sum(DIURETIC_6mo_after),
      quad = sum(QUAD_6mo_after),
      conv = sum(CONVENTIONAL_6mo_after),
      comp = sum(COMPREHENSIVE_6mo_after),
      .by = all_of(c("date_month", .group_by))
    ) |> 
    arrange(date_month) |>
    mutate(
      row = row_number(),
      across(c(sglt2:comp), \(x) x / n * 100, .names = "{.col}_perc"),
      across(c(n:comp), \(x) rnd(x / 5, 0) * 5),
      across(c(n:comp), \(x) if_else(x < 10 & x > 0, NA, x))
    )
  return(perc)
}