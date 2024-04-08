locals {
  project = "doctor-ilcsi" # Google Cloud Platform Project ID
}

resource "google_service_account" "account" {
  account_id   = "functions-invoker"
  display_name = "functions-invoker"
  project = local.project
}

data "archive_file" "function_archive" {
  type        = "zip"
  source_dir  = "../functions/src"
  output_path = "./tmp/function-source.zip"
}
resource "google_storage_bucket_object" "source_code" {
  count  = 1
  name   = "terraform-functions"
  bucket = "terraform-functions-bucket"
  source = data.archive_file.function_archive.output_path
}

# Cloud Functionの作成
resource "google_cloudfunctions2_function" "function" {
    depends_on = [
    google_storage_bucket_object.source_code,
  ]
  name        = "rakuten-scheduled-rename"
  location    = "us-central1"
  description = "楽天市場の商品に、定期的にクーポン情報を反映する関数"
  project     = local.project

  build_config {
    runtime     = "python311"
    entry_point = "main" # Set the entry point
    source {
      storage_source {
        object  = "terraform-functions"
        bucket = "terraform-functions-bucket"
      }
    }
  }
  service_config {
    min_instance_count = 1
    available_memory   = "256Mi"
    timeout_seconds    = 3600
    service_account_email = google_service_account.account.email
  }
}

# サービスアカウントに権限付与
resource "google_cloudfunctions2_function_iam_member" "invoker" {
  project        = google_cloudfunctions2_function.function.project
  location       = google_cloudfunctions2_function.function.location
  cloud_function = google_cloudfunctions2_function.function.name
  role           = "roles/cloudfunctions.invoker"
  member         = "serviceAccount:${google_service_account.account.email}"
}

resource "google_cloud_run_service_iam_member" "cloud_run_invoker" {
  project  = google_cloudfunctions2_function.function.project
  location = google_cloudfunctions2_function.function.location
  service  = google_cloudfunctions2_function.function.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.account.email}"
}

resource "google_cloud_scheduler_job" "invoke_cloud_function" {
  name        = "invoke-rakuten-scheduled-rename"
  description = "Schedule the HTTPS trigger for cloud function"
  schedule    = "0 1 * * *" 
  project     = google_cloudfunctions2_function.function.project
  region      = google_cloudfunctions2_function.function.location

  http_target {
    uri         = google_cloudfunctions2_function.function.service_config[0].uri
    http_method = "POST"
    oidc_token {
      audience              = "${google_cloudfunctions2_function.function.service_config[0].uri}/"
      service_account_email = google_service_account.account.email
    }
  }
}