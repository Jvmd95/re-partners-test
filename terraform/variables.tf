variable "project_id" {
  description = "The GCP Project ID where resources will be deployed"
  type        = string

  validation {
    condition     = length(var.project_id) > 0 && can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "Project ID must be 6-30 characters, start with a letter, and contain only lowercase letters, numbers, and hyphens."
  }
}

variable "region" {
  description = "The Google Cloud region for resource deployment"
  type        = string
  default     = "europe-west1"

  validation {
    condition     = can(regex("^[a-z]+-[a-z]+[0-9]+$", var.region))
    error_message = "Region must be a valid GCP region (e.g., europe-west1, us-central1)."
  }
}

variable "environment" {
  description = "Environment name (dev, staging, prod) - affects resource settings"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "dataflow_machine_type" {
  description = "Machine type for Dataflow workers"
  type        = string
  default     = "n1-standard-4"
}

variable "dataflow_max_workers" {
  description = "Maximum number of Dataflow workers for autoscaling"
  type        = number
  default     = 10

  validation {
    condition     = var.dataflow_max_workers >= 1 && var.dataflow_max_workers <= 1000
    error_message = "Max workers must be between 1 and 1000."
  }
}

variable "dataflow_num_workers" {
  description = "Initial number of Dataflow workers"
  type        = number
  default     = 1

  validation {
    condition     = var.dataflow_num_workers >= 1
    error_message = "Num workers must be at least 1."
  }
}

variable "bigquery_retention_days" {
  description = "Number of days to retain data in BigQuery tables (for time travel)"
  type        = number
  default     = 7

  validation {
    condition     = var.bigquery_retention_days >= 1 && var.bigquery_retention_days <= 7
    error_message = "BigQuery retention must be between 1 and 7 days."
  }
}

variable "bigquery_partition_expiration_days" {
  description = "Number of days after which partitions expire (0 = never)"
  type        = number
  default     = 0 # No expiration by default

  validation {
    condition     = var.bigquery_partition_expiration_days >= 0
    error_message = "Partition expiration must be >= 0 days."
  }
}

variable "pubsub_ack_deadline_seconds" {
  description = "Pub/Sub acknowledgement deadline in seconds"
  type        = number
  default     = 60

  validation {
    condition     = var.pubsub_ack_deadline_seconds >= 10 && var.pubsub_ack_deadline_seconds <= 600
    error_message = "Ack deadline must be between 10 and 600 seconds."
  }
}

variable "alert_notification_channels" {
  description = "List of notification channel IDs for alerts (e.g., email, Slack, PagerDuty)"
  type        = list(string)
  default     = []
}

variable "alert_error_rate_threshold" {
  description = "Number of errors per minute to trigger high error rate alert"
  type        = number
  default     = 10
}

variable "alert_low_throughput_threshold" {
  description = "Minimum orders per 5 minutes before low throughput alert triggers"
  type        = number
  default     = 1
}

variable "alert_dlq_accumulation_threshold" {
  description = "Number of unacked DLQ messages to trigger accumulation alert"
  type        = number
  default     = 100
}
