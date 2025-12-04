
-- =============================================================================
-- ORDERS TABLE
-- =============================================================================
CREATE TABLE `ecommerce_events.orders` (
  event_type STRING NOT NULL,
  order_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  order_date TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  total_amount FLOAT64 NOT NULL,
  items ARRAY<STRUCT<
    product_id STRING,
    product_name STRING,
    quantity INT64,
    price FLOAT64
  >>,
  shipping_address STRUCT<
    street STRING,
    city STRING,
    country STRING
  >,
  ingestion_timestamp TIMESTAMP
)
PARTITION BY DATE(order_date)
CLUSTER BY customer_id, status;

-- =============================================================================
-- INVENTORY LOGS TABLE
-- =============================================================================
CREATE TABLE `ecommerce_events.inventory_logs` (
  event_type STRING NOT NULL,
  inventory_id STRING NOT NULL,
  product_id STRING NOT NULL,
  warehouse_id STRING NOT NULL,
  quantity_change INT64 NOT NULL,
  reason STRING NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  ingestion_timestamp TIMESTAMP
)
PARTITION BY DATE(timestamp)
CLUSTER BY warehouse_id, product_id;

-- =============================================================================
-- USER ACTIVITY TABLE
-- =============================================================================
CREATE TABLE `ecommerce_events.user_activity` (
  event_type STRING NOT NULL,
  user_id STRING NOT NULL,
  activity_type STRING NOT NULL,
  ip_address STRING,
  user_agent STRING,
  timestamp TIMESTAMP NOT NULL,
  metadata STRUCT<
    session_id STRING,
    platform STRING
  >,
  ingestion_timestamp TIMESTAMP
)
PARTITION BY DATE(timestamp)
CLUSTER BY user_id, activity_type;

