# E-Commerce Streaming Pipeline

A real-time data pipeline that processes e-commerce events using Dataflow. It takes events from Pub/Sub, validates them, and writes to BigQuery + GCS


Three event types:
- Orders
- Inventory
- User Activity

Events are validated. Bad data goes to a dead letter queue


## How to Run

### Prerequisites

- Python 3.9+
- GCP project
- gcloud CLI
- Cloud Build

### 1. Run Tests

```bash
pytest tests/ -v
```

### 2. Deploy Infrastructure

```bash
cd terraform
terraform init
terraform apply -var="project_id=YOUR_PROJECT_ID"
```

This creates:
- Pub/Sub topics (events + DLQ)
- GCS buckets (data lake + staging)
- BigQuery dataset with 3 tables
- Service account with least-privilege IAM
- Monitoring alerts

### 3. Build & Deploy Pipeline

```bash
# Build Docker image
cd app
docker build -t europe-west1-docker.pkg.dev/YOUR_PROJECT/dataflow-templates/dataflow-pipeline:latest .

# Push to Artifact Registry
docker push europe-west1-docker.pkg.dev/YOUR_PROJECT/dataflow-templates/dataflow-pipeline:latest

# Create Flex Template
gcloud dataflow flex-template build \
  gs://YOUR_PROJECT-dataflow-templates/templates/ecommerce-pipeline.json \
  --image europe-west1-docker.pkg.dev/YOUR_PROJECT/dataflow-templates/dataflow-pipeline:latest \
  --sdk-language PYTHON \
  --metadata-file metadata.json
```

### 4. Launch the Pipeline

```bash
gcloud dataflow flex-template run ecommerce-pipeline \
  --template-file-gcs-location gs://YOUR_PROJECT-dataflow-templates/templates/ecommerce-pipeline.json \
  --region europe-west1 \
  --parameters input_subscription=projects/YOUR_PROJECT/subscriptions/backend-events-topic-sub \
  --parameters output_bucket=gs://YOUR_PROJECT-events-output \
  --parameters bq_dataset=YOUR_PROJECT:ecommerce_events
```


## Data Model

All schemas are defined in `schemas.json` — used by both Terraform (table creation) and Python (validation).

### Orders
| Field | Type | Notes |
|-------|------|-------|
| order_id | STRING | Primary identifier |
| customer_id | STRING | Clustered for fast lookups |
| order_date | TIMESTAMP | Partition key (daily) |
| status | STRING | pending/processing/shipped/delivered |
| total_amount | FLOAT64 | |
| items | RECORD[] | Nested array of products |
| shipping_address | RECORD | Nested struct |

### Inventory Logs
| Field | Type | Notes |
|-------|------|-------|
| inventory_id | STRING | |
| product_id | STRING | Clustered |
| warehouse_id | STRING | Clustered |
| quantity_change | INT64 | Positive or negative |
| reason | STRING | restock/sale/return/damage |
| timestamp | TIMESTAMP | Partition key |

### User Activity
| Field | Type | Notes |
|-------|------|-------|
| user_id | STRING | Clustered |
| activity_type | STRING | login/logout/view_product/add_to_cart |
| timestamp | TIMESTAMP | Partition key |
| metadata | RECORD | Optional session info |

## Validation

The pipeline validates events strictly:

- Missing required fields → DLQ
- Invalid enum values (e.g., `status: "banana"`) → DLQ
- Wrong types (e.g., `quantity: "five"`) → DLQ
- Empty arrays (e.g., order with no items) → DLQ

This keeps BigQuery clean. You can inspect the DLQ to debug bad upstream data.

## Design Decisions

**Why Dataflow over Spark Streaming?**
- Native GCP integration (Pub/Sub, BigQuery)
- Autoscaling built-in
- Managed infrastructure

**Why Flex Templates?**
- Docker packaging = any Python dependency works
- Version control via image tags
- Easy to launch from CLI, Console, or Terraform

**Why Streaming Inserts for BigQuery?**
- Simple and reliable
- Works well for moderate throughput
- Note: For higher throughput, could switch to Storage Write API

**Why strict validation?**
- Bad data shouldn't silently enter the warehouse
- DLQ preserves the original message for debugging
- Schema mismatches are caught early, not at query time

## Backfill Mode

The pipeline can also run in batch mode for historical data:

```bash
python app/main.py \
  --runner=DataflowRunner \
  --project=YOUR_PROJECT \
  --is_backfill \
  --s3_path=gs://historical-bucket/data/*.json \
  ...
```

This reads from GCS files instead of Pub/Sub. Same validation, same outputs.

## CI/CD

`cloudbuild.yaml` runs on every push:

1. Lint (flake8)
2. Test (pytest)
3. Build Docker image
4. Push to Artifact Registry
5. Create Flex Template

Tests must pass before the image is built.

## What I'd Improve

If I had more time:

1. **Deduplication** — Idempotent writes to handle replays
2. **More metrics** — Latency percentiles, payload sizes
3. **Alerting integration** — Actually configure Slack/PagerDuty notification channels
