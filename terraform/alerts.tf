resource "google_monitoring_alert_policy" "high_error_rate" {
  display_name = "[${upper(var.environment)}] Dataflow Pipeline - High Error Rate"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  documentation {
    content   = <<-DOC
      ## High Error Rate Alert
      
      **Severity**: Critical
      
      **Description**: The pipeline is experiencing a high rate of parsing/validation errors.
      Events are being routed to the Dead Letter Queue (DLQ).
      
      **Impact**: Data loss if errors persist. DLQ may accumulate.
      
      **Investigation Steps**:
      1. Check Dataflow job logs in Cloud Logging
      2. Review DLQ messages for error patterns
      3. Check upstream data source for schema changes
      4. Verify pipeline code hasn't regressed
      
      **Runbook**: https://docs.example.com/runbooks/high-error-rate
    DOC
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "Error count > ${var.alert_error_rate_threshold} per minute"

    condition_threshold {
      filter          = "resource.type=\"dataflow_job\" AND metric.type=\"custom.googleapis.com/dataflow/num_errors\""
      comparison      = "COMPARISON_GT"
      threshold_value = var.alert_error_rate_threshold
      duration        = "60s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "1800s" # 30 minutes
  }

  notification_channels = var.alert_notification_channels

  user_labels = {
    severity    = "critical"
    team        = "data-engineering"
    environment = var.environment
  }

  depends_on = [google_project_service.enabled_apis]
}

resource "google_monitoring_alert_policy" "low_throughput" {
  display_name = "[${upper(var.environment)}] Dataflow Pipeline - Low Throughput"
  project      = var.project_id
  combiner     = "OR"
  enabled      = var.environment == "prod" ? true : false # Only in prod

  documentation {
    content   = <<-DOC
      ## Low Throughput Alert
      
      **Severity**: Warning
      
      **Description**: The pipeline is processing fewer orders than expected.
      This could indicate upstream issues or pipeline stalls.
      
      **Impact**: Business metrics may be delayed or incomplete.
      
      **Investigation Steps**:
      1. Check Pub/Sub subscription for pending messages
      2. Verify upstream event producers are healthy
      3. Check Dataflow job status and autoscaling
      4. Review worker logs for processing delays
      
      **Runbook**: https://docs.example.com/runbooks/low-throughput
    DOC
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "Less than ${var.alert_low_throughput_threshold} order(s) in 5 minutes"

    condition_threshold {
      filter          = "resource.type=\"dataflow_job\" AND metric.type=\"custom.googleapis.com/dataflow/num_orders\""
      comparison      = "COMPARISON_LT"
      threshold_value = var.alert_low_throughput_threshold
      duration        = "300s" # 5 minutes

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "3600s" # 1 hour
  }

  notification_channels = var.alert_notification_channels

  user_labels = {
    severity    = "warning"
    team        = "data-engineering"
    environment = var.environment
  }

  depends_on = [google_project_service.enabled_apis]
}

resource "google_monitoring_alert_policy" "dlq_accumulation" {
  display_name = "[${upper(var.environment)}] Pipeline DLQ - Message Accumulation"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  documentation {
    content   = <<-DOC
      ## DLQ Accumulation Alert
      
      **Severity**: Warning
      
      **Description**: The Dead Letter Queue has accumulated too many unacknowledged messages.
      This indicates persistent data quality issues that need investigation.
      
      **Impact**: Data loss if DLQ is not processed. Potential compliance issues.
      
      **Investigation Steps**:
      1. Pull sample messages from DLQ subscription
      2. Analyze error patterns (JSON parsing, validation, schema)
      3. Identify root cause in upstream systems
      4. Consider backfill after fixing the issue
      
      **Runbook**: https://docs.example.com/runbooks/dlq-accumulation
    DOC
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "DLQ has > ${var.alert_dlq_accumulation_threshold} unacked messages"

    condition_threshold {
      filter          = "resource.type=\"pubsub_subscription\" AND resource.labels.subscription_id=\"backend-events-dlq-sub\" AND metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\""
      comparison      = "COMPARISON_GT"
      threshold_value = var.alert_dlq_accumulation_threshold
      duration        = "300s" # 5 minutes

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "7200s" # 2 hours
  }

  notification_channels = var.alert_notification_channels

  user_labels = {
    severity    = "warning"
    team        = "data-engineering"
    environment = var.environment
  }

  depends_on = [
    google_project_service.enabled_apis,
    google_pubsub_subscription.dlq_sub
  ]
}

resource "google_monitoring_alert_policy" "bq_write_failures" {
  display_name = "[${upper(var.environment)}] BigQuery - Streaming Insert Errors"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  documentation {
    content   = <<-DOC
      ## BigQuery Write Failures Alert
      
      **Severity**: Critical
      
      **Description**: BigQuery streaming inserts are failing. Data is not being persisted.
      
      **Impact**: Data loss. Analytics dashboards will show stale data.
      
      **Investigation Steps**:
      1. Check BigQuery error logs in Cloud Logging
      2. Verify table schema matches pipeline schema
      3. Check BigQuery quotas and limits
      4. Verify IAM permissions for service account
      5. Check for table-level issues (corrupted, deleted)
      
      **Common Causes**:
      - Schema mismatch between pipeline and table
      - Quota exceeded
      - Table deleted or access revoked
      - Invalid data types in payload
      
      **Runbook**: https://docs.example.com/runbooks/bq-write-failures
    DOC
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "Any BigQuery streaming insert errors"

    condition_threshold {
      filter          = "resource.type=\"bigquery_resource\" AND metric.type=\"bigquery.googleapis.com/storage/streaming_insert_count\" AND metric.labels.error_type!=\"\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "60s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "1800s" # 30 minutes
  }

  notification_channels = var.alert_notification_channels

  user_labels = {
    severity    = "critical"
    team        = "data-engineering"
    environment = var.environment
  }

  depends_on = [google_project_service.enabled_apis]
}

resource "google_monitoring_alert_policy" "dataflow_job_failed" {
  display_name = "[${upper(var.environment)}] Dataflow Job - Failed or Stopped"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  documentation {
    content   = <<-DOC
      ## Dataflow Job Failed Alert
      
      **Severity**: Critical
      
      **Description**: The Dataflow streaming job has failed or stopped unexpectedly.
      
      **Impact**: Complete data pipeline outage. No events are being processed.
      
      **Immediate Actions**:
      1. Check Dataflow job status in Console
      2. Review job logs for failure reason
      3. Restart the job if appropriate
      4. Check for infrastructure issues (quotas, permissions)
      
      **Runbook**: https://docs.example.com/runbooks/dataflow-job-failed
    DOC
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "Dataflow job is not running"

    condition_threshold {
      filter          = "resource.type=\"dataflow_job\" AND metric.type=\"dataflow.googleapis.com/job/is_failed\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "60s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "3600s" # 1 hour
  }

  notification_channels = var.alert_notification_channels

  user_labels = {
    severity    = "critical"
    team        = "data-engineering"
    environment = var.environment
  }

  depends_on = [google_project_service.enabled_apis]
}
