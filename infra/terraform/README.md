# Terraform — Alpha AML BigQuery bridge (thin)

Manages only:
- BigQuery dataset `aml_analytics`
- GCS staging bucket `alpha-aml-staging` (location **EU**, + **7-day lifecycle delete**)
- Least-privilege IAM for `aml-bq-sync@…` (Data Editor, Job User, Object Admin, Legacy Bucket Reader)

Does **not** manage the Oracle VM, Docker Compose, or dual-dbt.

## Prerequisites (this VM)

1. Terraform CLI ≥ 1.5 (**linux_arm64** on this Oracle aarch64 VM)
2. `gcloud` CLI; project `alpha-aml`
3. Auth as a project owner (not the Airflow sync SA JSON):
   ```bash
   gcloud auth login --no-launch-browser
   # ADC is preferred; if application-default login hits a scope crash, use:
   export GOOGLE_OAUTH_ACCESS_TOKEN="$(gcloud auth print-access-token)"
   ```
4. Billing enabled on project `alpha-aml`

> The Airflow SA JSON (`secrets/gcp-sa.json`) is for the **runtime DAG** only.

## Apply (import existing resources first)

Dataset / bucket / project IAM were created in the console. Import once so Terraform adopts state without recreating:

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # gcs_location must stay EU
terraform init

terraform import google_bigquery_dataset.aml_analytics projects/alpha-aml/datasets/aml_analytics
terraform import google_storage_bucket.staging alpha-aml-staging
terraform import google_project_iam_member.bq_data_editor "alpha-aml roles/bigquery.dataEditor serviceAccount:aml-bq-sync@alpha-aml.iam.gserviceaccount.com"
terraform import google_project_iam_member.bq_job_user "alpha-aml roles/bigquery.jobUser serviceAccount:aml-bq-sync@alpha-aml.iam.gserviceaccount.com"
# Bucket-level objectAdmin may be created on first apply if only project-level binding exists:
# terraform import google_storage_bucket_iam_member.staging_object_admin "b/alpha-aml-staging roles/storage.objectAdmin serviceAccount:aml-bq-sync@alpha-aml.iam.gserviceaccount.com"

terraform plan    # expect 0 destroy (location EU ≠ EUROPE-WEST1 would force bucket replace)
terraform apply
```

State files (`*.tfstate`, `terraform.tfvars`) are gitignored — keep them on the VM.

After apply, the bucket lifecycle should show: **Delete after 7 days**.

## Runtime retention (already in the DAG)

Even without bucket lifecycle, `export_to_bigquery` prunes `aml_gold/` objects older than `GCS_RETENTION_DAYS` (default 7) after each successful sync. BQ/GCS growth does **not** consume the VM's 45 GB disk.
