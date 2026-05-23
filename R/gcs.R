gcs_fs <- function() {
  key_path <- Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS",
                         "/mnt/project/sondreskarsten-d7d14-8486be2d085b.json")
  token_json <- system2("python3", c("-c", shQuote(sprintf(
    "from google.oauth2 import service_account; from google.auth.transport.requests import Request; c=service_account.Credentials.from_service_account_file('%s',scopes=['https://www.googleapis.com/auth/cloud-platform']); c.refresh(Request()); import json; print(json.dumps({'token':c.token,'expiry':c.expiry.isoformat()}))",
    key_path
  ))), stdout = TRUE)
  info <- jsonlite::fromJSON(token_json)
  arrow::GcsFileSystem$create(
    access_token = info$token,
    expiration = as.POSIXct(info$expiry, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
  )
}

BUCKET <- Sys.getenv("GCS_BUCKET", "sondre_brreg_data")

read_gcs <- function(path, columns = NULL) {
  fs <- gcs_fs()
  full <- paste0(BUCKET, "/", path)
  if (is.null(columns)) {
    arrow::read_parquet(fs$OpenInputFile(full))
  } else {
    arrow::read_parquet(fs$OpenInputFile(full), col_select = dplyr::all_of(columns))
  }
}

write_gcs <- function(df, path) {
  fs <- gcs_fs()
  full <- paste0(BUCKET, "/", path)
  out <- fs$OpenOutputStream(full)
  arrow::write_parquet(df, out, compression = "snappy")
  out$close()
  nrow(df)
}

list_gcs <- function(prefix) {
  fs <- gcs_fs()
  sel <- fs$GetFileInfo(arrow::FileSelector$create(
    paste0(BUCKET, "/", prefix), recursive = TRUE
  ))
  sel[vapply(sel, function(x) x$type == 2L, logical(1))]
}

latest_file <- function(prefix) {
  files <- list_gcs(prefix)
  paths <- vapply(files, function(x) x$path, character(1))
  paths <- sort(paths[grepl("\\.parquet$", paths)])
  tail(paths, 1)
}

save_targets_meta <- function() {
  meta_dir <- "_targets/meta"
  if (!dir.exists(meta_dir)) return(invisible(NULL))
  fs <- gcs_fs()
  files <- list.files(meta_dir, full.names = TRUE)
  for (f in files) {
    dest <- paste0(BUCKET, "/fleet_targets/_meta/", basename(f))
    out <- fs$OpenOutputStream(dest)
    writeBin(readBin(f, "raw", file.info(f)$size), out)
    out$close()
  }
  length(files)
}

restore_targets_meta <- function() {
  meta_dir <- "_targets/meta"
  dir.create(meta_dir, recursive = TRUE, showWarnings = FALSE)
  fs <- gcs_fs()
  tryCatch({
    sel <- fs$GetFileInfo(arrow::FileSelector$create(
      paste0(BUCKET, "/fleet_targets/_meta/"), recursive = FALSE
    ))
    files <- sel[vapply(sel, function(x) x$type == 2L, logical(1))]
    for (fi in files) {
      inp <- fs$OpenInputFile(fi$path)
      raw <- inp$Read(fi$size)
      writeBin(as.raw(raw), file.path(meta_dir, basename(fi$path)))
      inp$close()
    }
    length(files)
  }, error = function(e) 0L)
}
