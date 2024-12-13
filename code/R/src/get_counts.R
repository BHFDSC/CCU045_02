get_counts <- function(.data) {
  res <- .data |> 
    mutate(
      date = lubridate::floor_date(admission_date, unit = "months"), 
      .keep = "unused"
    ) |>
    summarise(
      total = n_distinct(subject_id),
      ref = n_distinct(subject_id[hf_subtype == "hfref"]),
      pef = n_distinct(subject_id[hf_subtype == "hfpef"]),
      uns = n_distinct(subject_id[hf_subtype == "unspecified"]),
      .by = date
    ) |>
    mutate(across(where(is.numeric), \(x) rnd(x / 5, 0) * 5)) |> 
    arrange(date)
  return(res)
}