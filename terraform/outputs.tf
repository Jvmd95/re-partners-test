output "pubsub_topic_id" {
  description = "Full resource ID of the events Pub/Sub topic"
  value       = google_pubsub_topic.events_topic.id
}

output "pubsub_topic_name" {
  description = "Name of the events Pub/Sub topic"
  value       = google_pubsub_topic.events_topic.name
}

output "pubsub_subscription_id" {
  description = "Full resource ID of the events Pub/Sub subscription"
  value       = google_pubsub_subscription.events_sub.id
}

output "pubsub_subscription_path" {
  description = "Full path of the subscription (for pipeline args)"
  value       = "projects/${var.project_id}/subscriptions/${google_pubsub_subscription.events_sub.name}"
}

output "dlq_topic_id" {
  description = "Full resource ID of the DLQ Pub/Sub topic"
  value       = google_pubsub_topic.dlq_topic.id
}

output "dlq_topic_path" {
  description = "Full path of the DLQ topic (for pipeline args)"
  value       = "projects/${var.project_id}/topics/${google_pubsub_topic.dlq_topic.name}"
}

output "data_lake_bucket_name" {
  description = "Name of the GCS bucket for event storage"
  value       = google_storage_bucket.data_lake.name
}

output "data_lake_bucket_url" {
  description = "GCS URL for the data lake bucket (for pipeline args)"
  value       = "gs://${google_storage_bucket.data_lake.name}"
}

output "staging_bucket_name" {
  description = "Name of the Dataflow staging bucket"
  value       = google_storage_bucket.dataflow_staging.name
}

output "staging_bucket_url" {
  description = "GCS URL for Dataflow staging (for pipeline args)"
  value       = "gs://${google_storage_bucket.dataflow_staging.name}"
}

output "templates_bucket_name" {
  description = "Name of the Flex Templates bucket"
  value       = google_storage_bucket.dataflow_templates.name
}

output "bigquery_dataset_id" {
  description = "ID of the BigQuery dataset"
  value       = google_bigquery_dataset.ecommerce.dataset_id
}

output "bigquery_dataset_path" {
  description = "Full path of the BigQuery dataset (for pipeline args)"
  value       = "${var.project_id}:${google_bigquery_dataset.ecommerce.dataset_id}"
}

output "orders_table_id" {
  description = "Full table ID for orders table"
  value       = "${var.project_id}:${google_bigquery_dataset.ecommerce.dataset_id}.${google_bigquery_table.orders.table_id}"
}

output "inventory_table_id" {
  description = "Full table ID for inventory logs table"
  value       = "${var.project_id}:${google_bigquery_dataset.ecommerce.dataset_id}.${google_bigquery_table.inventory_logs.table_id}"
}

output "user_activity_table_id" {
  description = "Full table ID for user activity table"
  value       = "${var.project_id}:${google_bigquery_dataset.ecommerce.dataset_id}.${google_bigquery_table.user_activity.table_id}"
}

output "artifact_registry_repository" {
  description = "Full path of the Artifact Registry repository"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.dataflow_templates.repository_id}"
}

output "docker_image_path" {
  description = "Full Docker image path for the pipeline"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.dataflow_templates.repository_id}/dataflow-pipeline"
}

output "dataflow_service_account_email" {
  description = "Email of the Dataflow worker service account"
  value       = google_service_account.dataflow_worker.email
}

output "dataflow_service_account_name" {
  description = "Full name of the Dataflow worker service account"
  value       = google_service_account.dataflow_worker.name
}

output "pipeline_launch_command" {
  description = "Example command to launch the Dataflow pipeline"
  value       = <<-EOT
    python app/main.py \
      --runner=DataflowRunner \
      --project=${var.project_id} \
      --region=${var.region} \
      --input_subscription=projects/${var.project_id}/subscriptions/${google_pubsub_subscription.events_sub.name} \
      --output_bucket=gs://${google_storage_bucket.data_lake.name} \
      --bq_dataset=${var.project_id}:${google_bigquery_dataset.ecommerce.dataset_id} \
      --temp_location=gs://${google_storage_bucket.dataflow_staging.name}/temp \
      --staging_location=gs://${google_storage_bucket.dataflow_staging.name}/staging \
      --service_account_email=${google_service_account.dataflow_worker.email}
  EOT
}
