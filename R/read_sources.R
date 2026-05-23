read_fartoy <- function() {
  path <- latest_file("fiskeridir/parsed/v1/state/")
  logger::log_info("fartoy: {path}")
  read_gcs(sub(paste0(BUCKET, "/"), "", path))
}

read_fangstdata <- function(years = 2020:2025) {
  dfs <- lapply(years, function(yr) {
    path <- sprintf("fangstdata/parsed/v1/year=%d/part-0.parquet", yr)
    tryCatch(read_gcs(path), error = function(e) NULL)
  })
  dfs <- Filter(Negate(is.null), dfs)
  logger::log_info("fangstdata: {length(dfs)} years, {sum(vapply(dfs, nrow, 0L))} rows")
  dplyr::bind_rows(dfs)
}

read_ais_stats <- function() {
  df <- read_gcs("ais/gold/ais_stats.parquet")
  logger::log_info("ais_stats: {nrow(df)} orgnrs, {sum(df$ais_days)} vessel-days")
  df
}

read_live <- function() {
  df <- read_gcs("ais/live/latest.parquet")
  logger::log_info("live: {nrow(df)} vessels")
  df
}

read_nsr <- function() {
  files <- list_gcs("ais/raw/statinfo/")
  paths <- vapply(files, function(x) x$path, character(1))
  sizes <- vapply(files, function(x) x$size, numeric(1))
  paths <- paths[grepl("\\.parquet$", paths)]
  sizes <- sizes[grepl("\\.parquet$", vapply(files, function(x) x$path, character(1)))]
  top <- head(paths[order(sizes, decreasing = TRUE)], 5)
  dfs <- lapply(top, function(p) {
    read_gcs(sub(paste0(BUCKET, "/"), "", p), columns = c("mmsino", "callsign"))
  })
  df <- dplyr::distinct(dplyr::bind_rows(dfs))
  df <- dplyr::filter(df, !is.na(callsign))
  logger::log_info("nsr: {nrow(df)} mmsi-callsign pairs")
  df
}

read_losore <- function() {
  df <- read_gcs("losore/state/snapshots.parquet")
  logger::log_info("losore: {nrow(df)} rettsstiftelser")
  df
}

read_finstat <- function() {
  fs <- gcs_fs()
  path <- "firm-deterioration/input_data_static/finstat.parquet"
  df <- arrow::read_parquet(
    fs$OpenInputFile(path),
    col_select = c("OffentligNr", "Regnskapsar", "RegnskapstypeKode",
                   "TotaleInntekter", "SumDriftskostnader", "Driftsresultat",
                   "Arsresultat", "SumEiendeler", "SumEK", "Lonnskostnad")
  )
  logger::log_info("finstat: {nrow(df)} rows")
  df
}

read_ledger_recent <- function(n_days = 60) {
  files <- list_gcs("integration/ledger/")
  paths <- vapply(files, function(x) x$path, character(1))
  paths <- sort(paths[grepl("\\.parquet$", paths)])
  paths <- tail(paths, n_days)
  dfs <- lapply(paths, function(p) {
    tryCatch(read_gcs(sub(paste0(BUCKET, "/"), "", p)), error = function(e) NULL)
  })
  dfs <- Filter(Negate(is.null), dfs)
  df <- dplyr::bind_rows(dfs)
  logger::log_info("ledger: {nrow(df)} events from {length(dfs)} days")
  df
}
