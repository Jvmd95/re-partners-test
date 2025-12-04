terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  # Load schemas from shared JSON file (single source of truth)
  schemas = jsondecode(file("${path.module}/../schemas.json"))

  # Resource naming convention
  resource_prefix = "${var.environment}-ecommerce"

  # Common labels for all resources
  common_labels = {
    environment = var.environment
    project     = "ecommerce-pipeline"
    managed_by  = "terraform"
  }
}

resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "dataflow.googleapis.com",
    "bigquery.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
    "artifactregistry.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "cloudbuild.googleapis.com",
  ])

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "dataflow_templates" {
  location      = var.region
  repository_id = "dataflow-templates"
  description   = "Docker repository for Dataflow Flex Templates"
  format        = "DOCKER"
  project       = var.project_id

  labels = local.common_labels

  depends_on = [google_project_service.enabled_apis]
}

resource "google_pubsub_topic" "events_topic" {
  name    = "backend-events-topic"
  project = var.project_id

  labels = local.common_labels

  # Message retention for replay capability
  message_retention_duration = "604800s" # 7 days

  depends_on = [google_project_service.enabled_apis]
}

resource "google_pubsub_subscription" "events_sub" {
  name    = "backend-events-topic-sub"
  topic   = google_pubsub_topic.events_topic.name
  project = var.project_id

  labels = local.common_labels

  # Acknowledgement deadline (must be > processing time)
  ack_deadline_seconds = var.pubsub_ack_deadline_seconds

  # Exactly-once delivery for data integrity
  enable_exactly_once_delivery = true

  # Message retention when subscription is paused
  message_retention_duration = "604800s" # 7 days

  # Retry policy for failed acknowledgements
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  # Expiration policy (never expire)
  expiration_policy {
    ttl = ""
  }
}

resource "google_pubsub_topic" "dlq_topic" {
  name    = "backend-events-dlq"
  project = var.project_id

  labels = merge(local.common_labels, {
    purpose = "dead-letter-queue"
  })

  message_retention_duration = "604800s" # 7 days

  depends_on = [google_project_service.enabled_apis]
}

resource "google_pubsub_subscription" "dlq_sub" {
  name    = "backend-events-dlq-sub"
  topic   = google_pubsub_topic.dlq_topic.name
  project = var.project_id

  labels = local.common_labels

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }
}

# Data Lake bucket
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-events-output"
  location      = var.region
  project       = var.project_id
  force_destroy = var.environment == "dev" ? true : false

  labels = local.common_labels

  # Uniform bucket-level access for security
  uniform_bucket_level_access = true

  # Versioning for data protection
  versioning {
    enabled = true
  }

  # Lifecycle rules for cost optimization
  lifecycle_rule {
    condition {
      age = 90 # days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365 # days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 730 # 2 years
    }
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }

  depends_on = [google_project_service.enabled_apis]
}

# Dataflow staging bucket
resource "google_storage_bucket" "dataflow_staging" {
  name          = "${var.project_id}-dataflow-staging"
  location      = var.region
  project       = var.project_id
  force_destroy = true

  labels = merge(local.common_labels, {
    purpose = "dataflow-staging"
  })

  uniform_bucket_level_access = true

  # Cleanup old staging files
  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.enabled_apis]
}

# Flex Template specs bucket
resource "google_storage_bucket" "dataflow_templates" {
  name          = "${var.project_id}-dataflow-templates"
  location      = var.region
  project       = var.project_id
  force_destroy = true

  labels = merge(local.common_labels, {
    purpose = "flex-templates"
  })

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  depends_on = [google_project_service.enabled_apis]
}

resource "google_bigquery_dataset" "ecommerce" {
  dataset_id  = "ecommerce_events"
  location    = var.region
  project     = var.project_id
  description = "E-commerce event data from streaming pipeline"

  labels = local.common_labels

  # Default table expiration (optional, set to null for no expiration)
  default_table_expiration_ms = null

  # Access control
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "WRITER"
    user_by_email = google_service_account.dataflow_worker.email
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  depends_on = [
    google_project_service.enabled_apis,
    google_service_account.dataflow_worker
  ]
}

resource "google_bigquery_table" "orders" {
  dataset_id          = google_bigquery_dataset.ecommerce.dataset_id
  table_id            = "orders"
  project             = var.project_id
  description         = "Customer order events with items and shipping details"
  deletion_protection = var.environment == "prod" ? true : false

  schema = jsonencode(local.schemas.orders)

  labels = local.common_labels

  time_partitioning {
    type                     = "DAY"
    field                    = "order_date"
    expiration_ms            = var.bigquery_partition_expiration_days * 24 * 60 * 60 * 1000
    require_partition_filter = var.environment == "prod" ? true : false
  }

  clustering = ["customer_id", "status"]
}

resource "google_bigquery_table" "inventory_logs" {
  dataset_id          = google_bigquery_dataset.ecommerce.dataset_id
  table_id            = "inventory_logs"
  project             = var.project_id
  description         = "Inventory change events for stock tracking"
  deletion_protection = var.environment == "prod" ? true : false

  schema = jsonencode(local.schemas.inventory_logs)

  labels = local.common_labels

  time_partitioning {
    type                     = "DAY"
    field                    = "timestamp"
    expiration_ms            = var.bigquery_partition_expiration_days * 24 * 60 * 60 * 1000
    require_partition_filter = var.environment == "prod" ? true : false
  }

  clustering = ["warehouse_id", "product_id", "reason"]
}

resource "google_bigquery_table" "user_activity" {
  dataset_id          = google_bigquery_dataset.ecommerce.dataset_id
  table_id            = "user_activity"
  project             = var.project_id
  description         = "User activity events for analytics and personalization"
  deletion_protection = var.environment == "prod" ? true : false

  schema = jsonencode(local.schemas.user_activity)

  labels = local.common_labels

  time_partitioning {
    type                     = "DAY"
    field                    = "timestamp"
    expiration_ms            = var.bigquery_partition_expiration_days * 24 * 60 * 60 * 1000
    require_partition_filter = var.environment == "prod" ? true : false
  }

  clustering = ["user_id", "activity_type"]
}

# Views for common queries
resource "google_bigquery_table" "orders_daily_summary" {
  dataset_id          = google_bigquery_dataset.ecommerce.dataset_id
  table_id            = "v_orders_daily_summary"
  project             = var.project_id
  description         = "Daily order summary view for analytics dashboards"
  deletion_protection = false

  labels = merge(local.common_labels, {
    type = "view"
  })

  view {
    query          = <<-SQL
      SELECT
        DATE(order_date) AS order_day,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value,
        COUNTIF(status = 'pending') AS pending_orders,
        COUNTIF(status = 'shipped') AS shipped_orders,
        COUNTIF(status = 'delivered') AS delivered_orders
      FROM `${var.project_id}.${google_bigquery_dataset.ecommerce.dataset_id}.orders`
      WHERE order_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
      GROUP BY order_day
      ORDER BY order_day DESC
    SQL
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.orders]
}

resource "google_bigquery_table" "inventory_low_stock" {
  dataset_id          = google_bigquery_dataset.ecommerce.dataset_id
  table_id            = "v_inventory_low_stock"
  project             = var.project_id
  description         = "Products with negative inventory changes (potential stockouts)"
  deletion_protection = false

  labels = merge(local.common_labels, {
    type = "view"
  })

  view {
    query          = <<-SQL
      SELECT
        product_id,
        warehouse_id,
        SUM(quantity_change) AS net_quantity_change,
        COUNT(*) AS transaction_count,
        MAX(timestamp) AS last_update
      FROM `${var.project_id}.${google_bigquery_dataset.ecommerce.dataset_id}.inventory_logs`
      WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
      GROUP BY product_id, warehouse_id
      HAVING SUM(quantity_change) < 0
      ORDER BY net_quantity_change ASC
    SQL
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.inventory_logs]
}

resource "google_bigquery_table" "user_engagement" {
  dataset_id          = google_bigquery_dataset.ecommerce.dataset_id
  table_id            = "v_user_engagement"
  project             = var.project_id
  description         = "User engagement metrics by activity type"
  deletion_protection = false

  labels = merge(local.common_labels, {
    type = "view"
  })

  view {
    query          = <<-SQL
      SELECT
        DATE(timestamp) AS activity_day,
        activity_type,
        metadata.platform AS platform,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(*) AS total_events
      FROM `${var.project_id}.${google_bigquery_dataset.ecommerce.dataset_id}.user_activity`
      WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
      GROUP BY activity_day, activity_type, platform
      ORDER BY activity_day DESC, total_events DESC
    SQL
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.user_activity]
}
