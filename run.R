library(targets)
library(logger)

source("R/gcs.R")

log_info("fleet-targets pipeline starting")

gcs_auth()
log_info("GCS auth OK (bucket={BUCKET})")

n_restored <- restore_targets_meta()
log_info("restored {n_restored} metadata files from GCS")

tar_make(callr_function = NULL)

log_info("pipeline complete — saving metadata to GCS")
n_saved <- save_targets_meta()
log_info("saved {n_saved} metadata files to GCS")

manifest <- tar_read(manifest)
for (i in seq_len(nrow(manifest))) {
  log_info("{manifest$output[i]}: {manifest$rows[i]} rows x {manifest$cols[i]} cols")
}

log_info("fleet-targets DONE")
