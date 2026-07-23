# Thin Terraform for the Alpha AML BigQuery export bridge.
# Manages only: BigQuery dataset, staging GCS bucket (+ lifecycle),
# and least-privilege IAM for the existing sync service account.
#
# Not in scope: VPC, GKE, full project bootstrap, dual-dbt.

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.40"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "aml_analytics" {
  dataset_id                 = var.bq_dataset
  friendly_name              = "Alpha AML Gold analytics"
  description                = "Daily Gold sync from Postgres (export_to_bigquery DAG)"
  location                   = var.bq_location
  delete_contents_on_destroy = false
}

resource "google_storage_bucket" "staging" {
  name                        = var.gcs_bucket
  location                    = var.gcs_location
  force_destroy               = false
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Belt-and-suspenders with the DAG-side prune (GCS_RETENTION_DAYS).
  lifecycle_rule {
    condition {
      age = var.gcs_retention_days
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    project = "alpha-aml"
    purpose = "gold-staging"
  }
}

# Bind least-privilege roles to the *existing* sync SA (created in console).
# Do not grant Owner/Editor.
resource "google_project_iam_member" "bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${var.sync_sa_email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.sync_sa_email}"
}

resource "google_storage_bucket_iam_member" "staging_object_admin" {
  bucket = google_storage_bucket.staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.sync_sa_email}"
}

# Needed so Terraform (or ops) can manage bucket lifecycle metadata.
resource "google_storage_bucket_iam_member" "staging_legacy_reader" {
  bucket = google_storage_bucket.staging.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${var.sync_sa_email}"
}

output "bq_dataset_id" {
  value = google_bigquery_dataset.aml_analytics.dataset_id
}

output "gcs_bucket_name" {
  value = google_storage_bucket.staging.name
}

output "sync_sa_email" {
  value = var.sync_sa_email
}
