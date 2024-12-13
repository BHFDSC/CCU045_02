get_temporal_trends_overall <- function(.data,
                                        .object,
                                        .ep_var, 
                                        .tte_var,
                                        .date_var,
                                        .start_date = "2019-01-01") {
  make_tibble <- function(lst) {
    lst |>
      map(\(x) as_tibble(x)) %>%
      map2(names(.), \(x, y) add_column(x, term = rep(y, nrow(x)))) |> 
      bind_rows()
  }
  
  object_res <- .object |> 
    broom::tidy(exponentiate = FALSE) |>
    mutate(row = row_number())
  
  int <- object_res |> filter(str_detect(term, "Int"))
  grp <- object_res |> filter(!str_detect(term, "Int"))
  int_lst <- lst(int)
  
  grps <- grp %>%
    split(.$row) |> 
    map(\(x) bind_rows(int, x)) %>%
    append(int_lst, .) |>
    map(\(x) mutate(x, contrast = 1))
  
  coef <- grps |>
    map(\(x) sum(x$estimate))
  
  variance <- 
    replicate(nrow(object_res), object_res, simplify = FALSE) |>
    map2(
      grps,
      \(x, y) left_join(
        x, y, by = join_by(term, estimate, std.error, statistic, p.value, row)
      )
    ) |>
    map(
      \(x) mutate(x, contrast = ifelse(is.na(contrast), 0, contrast)) |>
        pull(contrast)
    ) %>%
    map(~t(.) %*% vcov(.object) %*% .)
  
  se <- variance |>
    map(\(x) sqrt(diag(x)))
  
  ci <-
    map2(coef, se, \(x, y) x + c(-1.96, 1.96) * y) |>
    map(
      \(x) as_tibble(x) |>
        mutate(name = c("lci", "uci")) |>
        pivot_wider(id_cols = everything())
    )

  # Date name
  dm <- .data |> select(all_of(.date_var)) |> names()

  coef_tbl <- make_tibble(coef) |> rename(coef = value)
  ci_tbl <- make_tibble(ci)
  se_tbl <- se |> map(\(x) as_tibble_col(x, column_name = "se")) |> bind_rows()
  comb_tbl <-
    left_join(coef_tbl, ci_tbl, by = "term") |>
    bind_cols(se_tbl) |>
    relocate(term, .before = 1) |>
    mutate(
      var = se^2,
      mutate(across(c(coef, lci, uci), \(x) exp(x) * 100))
    ) |>
    bind_cols(object_res |> dplyr::select(original = term)) |>
    filter(!str_detect(original, "age")) |>
    mutate(
      term = case_when(
        original == "(Intercept)" ~ .start_date,
        .default = str_remove_all(original, dm)
      )
    ) |>
    dplyr::select(term, inc_rate = coef, lci, uci, se) |>
    mutate(row = row_number())

  # Get numbers of events and person years
  n <- .data |>
    summarise(
      n = rnd(sum({{ .ep_var }}) / 5, 0) * 5, py = sum({{ .tte_var }}),
      .by = !!sym({{ .date_var }})
    ) |>
    arrange(!!sym({{ .date_var }})) |>
    mutate(term = as.character(!!sym(.date_var)), .keep = "unused")

  comb_tbl <- comb_tbl |> left_join(n, by = join_by(term))

  return(comb_tbl)
}