cat("=== DIAGNOSTICS ===\n")
cat(paste("R version:", R.version.string, "\n"))

for (pkg in c("targets", "arrow", "duckdb", "DBI", "logger", "googleCloudStorageR", "gargle", "googleAuthR", "dplyr", "jsonlite")) {
  ok <- tryCatch({ library(pkg, character.only = TRUE); "OK" }, error = function(e) paste("FAIL:", e$message))
  cat(sprintf("  %-25s %s\n", pkg, ok))
}

cat("\n=== GCS AUTH ===\n")
tryCatch({
  googleAuthR::gar_gce_auth()
  cat("  gar_gce_auth: OK\n")
}, error = function(e) cat(paste("  gar_gce_auth FAIL:", e$message, "\n")))

tryCatch({
  googleCloudStorageR::gcs_global_bucket("sondre_brreg_data")
  objs <- googleCloudStorageR::gcs_list_objects(prefix = "ais/gold/", detail = "summary")
  cat(sprintf("  gcs_list_objects: %d objects\n", nrow(objs)))
}, error = function(e) cat(paste("  gcs_list FAIL:", e$message, "\n")))

cat("\n=== DONE ===\n")
