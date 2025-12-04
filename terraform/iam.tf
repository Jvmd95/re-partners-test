resource "google_service_account" "dataflow_worker" {
  account_id   = "dataflow-worker-${var.environment}"
  display_name = "Dataflow Worker (${var.environment})"
  description  = "Service account for Dataflow pipeline workers - ${var.environment} environment"
  project      = var.project_id
}

locals {
  # Dataflow worker requires these roles
  dataflow_worker_roles = [
    # Core Dataflow operations
    "roles/dataflow.worker",

    # BigQuery - write data to tables
    "roles/bigquery.dataEditor",

    # BigQuery - run streaming insert jobs
    "roles/bigquery.jobUser",

    # GCS - read/write to staging and output buckets
    "roles/storage.objectAdmin",

    # Pub/Sub - read from subscription
    "roles/pubsub.subscriber",

    # Pub/Sub - write to DLQ topic
    "roles/pubsub.publisher",

    # Monitoring - write custom metrics
    "roles/monitoring.metricWriter",

    # Logging - write logs
    "roles/logging.logWriter",
  ]
}

resource "google_project_iam_member" "dataflow_worker_roles" {
  for_each = toset(local.dataflow_worker_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}



# Cloud Build needs these to deploy Flex Templates
resource "google_project_iam_member" "cloudbuild_dataflow_admin" {
  project = var.project_id
  role    = "roles/dataflow.admin"
  member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"

  depends_on = [google_project_service.enabled_apis]
}

resource "google_project_iam_member" "cloudbuild_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"

  depends_on = [google_project_service.enabled_apis]
}

resource "google_project_iam_member" "cloudbuild_artifact_registry" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"

  depends_on = [google_project_service.enabled_apis]
}

# Allow Cloud Build to act as the Dataflow worker service account
resource "google_service_account_iam_member" "cloudbuild_act_as" {
  service_account_id = google_service_account.dataflow_worker.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"

  depends_on = [google_project_service.enabled_apis]
}

data "google_project" "project" {
  project_id = var.project_id
}
