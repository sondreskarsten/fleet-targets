library(targets)

tar_option_set(
  packages = c("dplyr", "arrow", "duckdb", "DBI", "logger", "googleCloudStorageR"),
  format = "rds"
)

tar_source("R/")

list(
  tar_target(fartoy, read_fartoy()),
  tar_target(fangstdata, read_fangstdata()),
  tar_target(ais_stats, read_ais_stats()),
  tar_target(live, read_live()),
  tar_target(nsr, read_nsr()),
  tar_target(losore, read_losore()),
  tar_target(finstat_raw, read_finstat()),
  tar_target(ledger_recent, read_ledger_recent()),

  tar_target(finstat_clean, compute_finstat_clean(finstat_raw)),
  tar_target(bank_segments, compute_bank_segments(losore)),
  tar_target(latest_events, compute_latest_events(ledger_recent)),
  tar_target(catch_agg, compute_catch_agg(fangstdata, fartoy)),
  tar_target(ref_groups, compute_ref_groups(catch_agg)),

  tar_target(fleet_panel, materialize_fleet_panel(
    catch_agg, ais_stats, finstat_clean, live, nsr, bank_segments)),
  tar_target(portfolio_vessel, materialize_portfolio_vessel(
    fartoy, catch_agg, ais_stats, live, nsr, finstat_clean,
    bank_segments, ref_groups, latest_events)),
  tar_target(capacity_util, materialize_capacity_util(fleet_panel)),

  tar_target(upload_fleet_panel,
    write_gcs(fleet_panel, "ais/gold/fleet_panel.parquet"),
    deployment = "main"),
  tar_target(upload_portfolio_vessel,
    write_gcs(portfolio_vessel, "ais/gold/portfolio_vessel.parquet"),
    deployment = "main"),
  tar_target(upload_capacity_util,
    write_gcs(capacity_util, "ais/gold/capacity_utilization.parquet"),
    deployment = "main"),

  tar_target(manifest, {
    ts <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
    m <- data.frame(
      output = c("fleet_panel", "portfolio_vessel", "capacity_utilization"),
      rows = c(nrow(fleet_panel), nrow(portfolio_vessel), nrow(capacity_util)),
      cols = c(ncol(fleet_panel), ncol(portfolio_vessel), ncol(capacity_util)),
      uploaded_at = ts,
      stringsAsFactors = FALSE
    )
    write_gcs(m, sprintf("fleet_targets/manifests/%s.parquet", Sys.Date()))
    m
  })
)
