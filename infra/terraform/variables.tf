variable "project_id" {
  type        = string
  description = "GCP project ID"
  default     = "alpha-aml"
}

variable "region" {
  type        = string
  description = "Default provider region"
  default     = "europe-west1"
}

variable "bq_dataset" {
  type        = string
  default     = "aml_analytics"
}

variable "bq_location" {
  type        = string
  description = "BigQuery dataset location (EU multi-region or a region)"
  default     = "EU"
}

variable "gcs_bucket" {
  type        = string
  description = "Staging bucket for Gold Parquet (globally unique)"
  default     = "alpha-aml-staging"
}

variable "gcs_location" {
  type        = string
  default     = "EUROPE-WEST1"
}

variable "gcs_retention_days" {
  type        = number
  description = "Delete staging objects older than this many days"
  default     = 7
}

variable "sync_sa_email" {
  type        = string
  description = "Existing service account used by export_to_bigquery"
  default     = "aml-bq-sync@alpha-aml.iam.gserviceaccount.com"
}
