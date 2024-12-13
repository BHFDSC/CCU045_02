get_temporal_trends_by_subgroup <- function(.object, 
                                            .non_baseline_regex = c("hfpef", "unspecified"), 
                                            .groups = c("hfref", "hfpef", "unspecified"),
                                            .start_date = "2019-01-01",
                                            .date_var,
                                            .age_centered = TRUE) {
  # Get model matrix
  m <- model.matrix(.object)
  # Code all contrasts for centred age as 0 (we don't want to include this 
  # variable in contrasts)
  if (.age_centered == TRUE) {
    m[, "age_c"] <- 0   
  }
  # Get unique contrasts to get combinations we want
  m2 <- unique(m)
  # Matrix multiplication of contrasts with model coefficients (this gets the 
  # incidence rates for all the groups we're interested in)
  cf <- as.vector(m2 %*% coef(.object))
  # Get the variance for each of the incidence rates using matrix multiplication
  # with the variance-covariance matrix
  v <- as.list(as.data.frame(t(m2))) %>% map(\(x) t(x) %*% vcov(.object) %*% x)
  # Get standard errors
  se <- v |> map(\(x) sqrt(diag(x)))
  # Calculate confidence intervals
  ci <- map2(cf, se, \(x, y) x + c(-1.96, 1.96) * y) |>
    map(
      \(x) as_tibble(x) |>
        mutate(name = c("lci", "uci")) |>
        pivot_wider(id_cols = everything())
    ) |>
    bind_rows()
  # Incidence rates and 95% confidence intervals
  ir <- tibble(coef = cf) |> bind_cols(ci) |> mutate(coef = rnd(coef, 8))
  
  # Get ordering/grouping of the incidence rates by calculating it alongside
  # each of the coefficient names
  
  # Get `tibble` of model coefficients
  .object_res <- .object |> 
    broom::tidy(exponentiate = FALSE)
  
  # Vectors for loops
  baseline <- paste0("^", .date_var, "[0-9]{4}-[0-9]{2}-[0-9]{2}$")
  rgx <- append(baseline, .non_baseline_regex)
  grp <- .groups
  grp_bl  <- setdiff(grp, .non_baseline_regex)
  
  # Loop
  out_lst <- list()
  
  # This loop separates the model tibble by the groups you have specified in the
  # function arguments creating a list
  for(r in 1:length(rgx)) {
    res <- .object_res |>
      filter(str_detect(term, rgx[r])) |>
      dplyr::select(term, estimate) |>
      mutate(
        intercept = .object_res |> filter(str_detect(term, "Int")) |> pull(estimate),
        term = if_else(!str_detect(term, .date_var), paste0(.date_var, .start_date), term),
        term = str_remove_all(term, "[:].+"),
        group = grp[r],
        start = if_else(str_detect(term, paste0(.date_var, .start_date)), estimate, NA)
      ) |>
      fill(start, .direction = "down")
    out_lst[[r]] <- res
  }
  
  # This code calculates the incidence rates manually for each of the groups 
  # in the dataframe and joins it to the (log) incidence rates calculated using
  # the matrix multiplication
  res_fnl <- out_lst |>
    map(
      \(x) 
      left_join(
        x, out_lst[[1]] |> 
          dplyr::select(term, bl = estimate), by = join_by(term)
      )
    ) |>
    bind_rows() |>
    mutate(
      bl = if_else(is.na(bl), 0, bl),
      coef = case_when(
        group != grp_bl & estimate != start ~ estimate + intercept + bl + start,
        group != grp_bl & estimate == start ~ estimate + intercept + bl,
        .default = estimate + intercept
      ),
      # The rounding is necessary to get the coefficients to join correctly
      # to those calculated using matrix multiplication
      coef = rnd(coef, 8)
    ) |>
    bind_rows(
      .object_res |> 
        filter(term == "(Intercept)") |>
        dplyr::select(term, coef = estimate, std.error)
    ) |> 
    left_join(ir, by = join_by(coef)) |>
    mutate(
      group = if_else(term == "(Intercept)", grp_bl, group),
      lci = if_else(term == "(Intercept)", coef - 1.96 * std.error, lci),
      uci = if_else(term == "(Intercept)", coef + 1.96 * std.error, uci),
      term = if_else(term == "(Intercept)", paste0(.date_var, .start_date), term),
      across(c(coef, lci, uci), \(x) exp(x) * 100)
    ) |>
    arrange(group, term)|>
    group_by(group) |>
    mutate(row = row_number()) |>
    ungroup() |>
    mutate(term = str_remove_all(term, .date_var)) |>
    dplyr::select(term, group, inc_rate = coef, lci, uci, row)
  
  return(res_fnl)
}