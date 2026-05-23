library(googleCloudStorageR)

BUCKET <- Sys.getenv("GCS_BUCKET", "sondre_brreg_data")

gcs_auth <- function() {
  key_path <- Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
  if (nzchar(key_path) && file.exists(key_path)) {
    googleCloudStorageR::gcs_auth(json_file = key_path)
  } else {
    gargle::credentials_gce()
    googleCloudStorageR::gcs_auth(token = gargle::token_fetch(
      scopes = "https://www.googleapis.com/auth/cloud-platform"
    ))
  }
  googleCloudStorageR::gcs_global_bucket(BUCKET)
}

read_gcs <- function(path, columns = NULL, bucket = BUCKET) {
  local <- tempfile(fileext = ".parquet")
  googleCloudStorageR::gcs_get_object(path, saveToDisk = local, bucket = bucket, overwrite = TRUE)
  if (is.null(columns)) {
    arrow::read_parquet(local)
  } else {
    arrow::read_parquet(local, col_select = dplyr::all_of(columns))
  }
}

write_gcs <- function(df, path, bucket = BUCKET) {
  local <- tempfile(fileext = ".parquet")
  arrow::write_parquet(df, local, compression = "snappy")
  googleCloudStorageR::gcs_upload(local, bucket = bucket, name = path,
                                   predefinedAcl = "projectPrivate")
  nrow(df)
}

list_gcs_files <- function(prefix, bucket = BUCKET) {
  objs <- googleCloudStorageR::gcs_list_objects(bucket = bucket, prefix = prefix)
  objs <- objs[grepl("\\.parquet$", objs$name), ]
  objs[order(objs$name), ]
}

latest_file <- function(prefix, bucket = BUCKET) {
  objs <- list_gcs_files(prefix, bucket)
  tail(objs$name, 1)
}

save_targets_meta <- function() {
  meta_dir <- "_targets/meta"
  if (!dir.exists(meta_dir)) return(0L)
  files <- list.files(meta_dir, full.names = TRUE)
  for (f in files) {
    googleCloudStorageR::gcs_upload(f, name = paste0("fleet_targets/_meta/", basename(f)),
                                    predefinedAcl = "projectPrivate")
  }
  length(files)
}

restore_targets_meta <- function() {
  meta_dir <- "_targets/meta"
  dir.create(meta_dir, recursive = TRUE, showWarnings = FALSE)
  objs <- tryCatch(
    googleCloudStorageR::gcs_list_objects(prefix = "fleet_targets/_meta/"),
    error = function(e) data.frame(name = character(0))
  )
  if (nrow(objs) == 0) return(0L)
  for (nm in objs$name) {
    googleCloudStorageR::gcs_get_object(nm, saveToDisk = file.path(meta_dir, basename(nm)),
                                         overwrite = TRUE)
  }
  nrow(objs)
}
