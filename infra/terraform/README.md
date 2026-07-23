# Terraform — Alpha AML BigQuery bridge (thin)

Manages only:
- BigQuery dataset `aml_analytics`
- GCS staging bucket (+ **7-day lifecycle delete**)
- Least-privilege IAM for `aml-bq-sync@…` (Data Editor, Job User, Object Admin)

Does **not** manage the Oracle VM, Docker Compose, or dual-dbt.

## Prerequisites

1. [Terraform CLI](https://developer.hashicorp.com/terraform/install) ≥ 1.5 on your laptop  
2. `gcloud auth application-default login` (or a user with rights to create dataset/bucket/IAM)  
3. Billing enabled on project `alpha-aml`

> The Airflow SA JSON is for the **runtime DAG**, not for Terraform apply. Apply as yourself (owner) from your laptop.

## Apply (import existing resources first)

The dataset, bucket, and IAM were created in the console. Import them once so Terraform adopts state without recreating:

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # edit if needed
terraform init

terraform import google_bigquery_dataset.aml_analytics projects/alpha-aml/datasets/aml_analytics
terraform import google_storage_bucket.staging alpha-aml-staging
terraform import google_project_iam_member.bq_data_editor "alpha-aml roles/bigquery.dataEditor serviceAccount:aml-bq-sync@alpha-aml.iam.gserviceaccount.com"
terraform import google_project_iam_member.bq_job_user "alpha-aml roles/bigquery.jobUser serviceAccount:aml-bq-sync@alpha-aml.iam.gserviceaccount.com"
terraform import google_storage_bucket_iam_member.staging_object_admin "b/alpha-aml-staging roles/storage.objectAdmin serviceAccount:aml-bq-sync@alpha-aml.iam.gserviceaccount.com"

terraform plan
terraform apply
```

After apply, the bucket should show a lifecycle rule: **Delete after 7 days**.

## Runtime retention (already in the DAG)

Even without bucket lifecycle IAM, `export_to_bigquery` prunes `aml_gold/` objects older than `GCS_RETENTION_DAYS` (default 7) after each successful sync.
