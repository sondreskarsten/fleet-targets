download_gcs <- function(path, bucket = BUCKET) {
  local <- file.path(tempdir(), gsub("/", "_", path))
  googleCloudStorageR::gcs_get_object(path, saveToDisk = local, bucket = bucket, overwrite = TRUE)
  local
}

read_fartoy <- function() {
  path <- latest_file("fiskeridir/parsed/v1/state/")
  logger::log_info("fartoy: {path}")
  read_gcs(path)
}

read_fangstdata_files <- function(years = 2020:2025) {
  paths <- c()
  for (yr in years) {
    p <- sprintf("fangstdata/parsed/v1/year=%d/part-0.parquet", yr)
    local <- tryCatch(download_gcs(p), error = function(e) NULL)
    if (!is.null(local)) paths <- c(paths, local)
  }
  logger::log_info("fangstdata: {length(paths)} year files downloaded")
  paths
}

read_ais_stats <- function() {
  df <- read_gcs("ais/gold/ais_stats.parquet")
  logger::log_info("ais_stats: {nrow(df)} orgnrs")
  df
}

read_live <- function() {
  df <- read_gcs("ais/live/latest.parquet")
  logger::log_info("live: {nrow(df)} vessels")
  df
}

read_nsr <- function() {
  objs <- list_gcs_files("ais/raw/statinfo/")
  objs <- objs[order(objs$size, decreasing = TRUE), ]
  top <- head(objs$name, 5)
  dfs <- lapply(top, function(p) read_gcs(p, columns = c("mmsino", "callsign")))
  df <- dplyr::distinct(dplyr::bind_rows(dfs))
  df <- dplyr::filter(df, !is.na(callsign))
  logger::log_info("nsr: {nrow(df)} mmsi-callsign pairs")
  df
}

download_losore <- function() {
  local <- download_gcs("losore/state/snapshots.parquet")
  sz <- file.info(local)$size / 1e6
  logger::log_info("losore: downloaded {round(sz, 1)} MB")
  local
}

download_finstat <- function() {
  local <- download_gcs("input_data_static/finstat.parquet", bucket = "firm-deterioration")
  sz <- file.info(local)$size / 1e6
  logger::log_info("finstat: downloaded {round(sz, 1)} MB")
  local
}

read_ledger_recent <- function(n_days = 60) {
  objs <- list_gcs_files("integration/ledger/")
  paths <- tail(objs$name, n_days)
  dfs <- lapply(paths, function(p) tryCatch(read_gcs(p), error = function(e) NULL))
  dfs <- Filter(Negate(is.null), dfs)
  df <- dplyr::bind_rows(dfs)
  logger::log_info("ledger: {nrow(df)} events from {length(dfs)} days")
  df
}
