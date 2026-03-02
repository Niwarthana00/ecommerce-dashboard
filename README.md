<div align="center">

# 🛒 Ecommerce Streaming Pipeline

**Real-time CDC pipeline — from raw transactions to analytics-ready data**

[![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Debezium](https://img.shields.io/badge/Debezium-CDC-red?style=flat)](https://debezium.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=postgresql&logoColor=white)](https://postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://docker.com/)

</div>

---

## Overview

This pipeline ingests the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from a source PostgreSQL database and streams it in real time through Kafka into a clean analytics warehouse — with no manual refresh needed.

```
Source DB
    │
    │  WAL (Write-Ahead Log)
    ▼
Debezium CDC  ──►  Kafka Topics  ──►  Spark Streaming
                                            │
                                            ▼
                                    Silver Tables  (ecommerce_db)
                                            │
                                            ▼
                                    Gold Materialized Views
```

Any insert or update in the source DB propagates to the gold analytics layer within ~10 seconds — automatically.

---

## Stack

| Layer | Tool |
|-------|------|
| Change Data Capture | Debezium 2.7 + PostgreSQL pgoutput |
| Message Broker | Apache Kafka (Confluent) |
| Stream Processing | Apache Spark 3.5 Structured Streaming |
| Storage | PostgreSQL 14+ |
| Orchestration | Docker Compose + Taskfile |

---

## Data Layers

### Silver — 7 tables
Raw, cleaned mirror of source tables. Upserted on every Kafka batch.

`customers` · `orders` · `order_items` · `order_payments` · `order_reviews` · `products` · `sellers`

### Gold — 7 materialized views
Aggregated and dashboard-ready. Auto-refreshed whenever orders or payments are updated.

| View | Contents |
|------|----------|
| `gold_sales_monthly` | Revenue, order volume, avg order value per month |
| `gold_sales_daily` | Day-level sales summary |
| `gold_customer_metrics` | Spending, order history, review scores per customer |
| `gold_product_performance` | Revenue, units sold, ratings per product |
| `gold_seller_performance` | Revenue, ratings, product count per seller |
| `gold_delivery_analytics` | On-time rate, avg delivery days, late deliveries |
| `gold_payment_analytics` | Payment method breakdown, installment patterns |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [PostgreSQL 14+](https://www.postgresql.org/download) — running locally
- [Taskfile](https://taskfile.dev) — task runner

```bash
npm install -g @go-task/cli
```

**PostgreSQL must have logical WAL enabled** — required for Debezium CDC.

Connect to PostgreSQL and run:

```sql
-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;

-- Grant replication permission to your user
ALTER USER postgres REPLICATION;
```

Then restart PostgreSQL for the changes to take effect:

```powershell
# Windows (run as Administrator)
net stop postgresql-x64-17
net start postgresql-x64-17
```

```bash
# Linux / macOS
sudo systemctl restart postgresql
```

Verify:

```sql
SHOW wal_level;  -- should return 'logical'
```

---

## Setup

### 1. Clone & configure

```bash
git clone https://github.com/your-username/ecommerce-dashboard.git
cd ecommerce-dashboard
cp .env.example .env
```

Edit `.env`:

```env
# Silver / Gold database
DB_URL=jdbc:postgresql://host.docker.internal:5432/ecommerce_db
DB_HOST=host.docker.internal
DB_NAME=ecommerce_db
DB_USER=postgres
DB_PASS=your_password

# Source database
SOURCE_DB_NAME=source_db
TOPIC_PREFIX=kafka_topic_prefix

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

> On Linux, replace `host.docker.internal` with `172.17.0.1`

### 2. Create the target database

```bash
psql -U postgres -c "CREATE DATABASE ecommerce_db;"
psql -U postgres -d ecommerce_db -f init.sql
psql -U postgres -d ecommerce_db -f gold_layer.sql
```

### 3. Load source data

Download the dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), place the CSV files in `csvs/`, then:

```bash
task load
```

This loads all CSVs into the source database.

### 4. Start the pipeline

```bash
task up
```

That's it. Debezium will snapshot the source DB, push all records through Kafka, and Spark will populate the silver and gold layers automatically.

---

## Task Commands

```bash
task up                # Start all services
task down              # Stop all services
task restart           # Stop then start
task build             # Rebuild Docker images
task load              # Load CSVs into source DB (once)

task status            # Container status
task connector         # Debezium connector status
task logs-spark        # Spark streaming logs
task logs-kafka        # Kafka logs
task logs-debezium     # Debezium connector logs

task db-source         # psql into source DB 
task db-silver         # psql into silver DB (ecommerce_db)
task refresh-gold      # Manually refresh all gold views

task clean             # Stop + delete all volumes
task clean-checkpoints # Clear Spark checkpoints
```

---

## Monitoring

| Service | URL |
|---------|-----|
| Spark Master UI | http://localhost:8081 |
| Spark Worker UI | http://localhost:8082 |
| Kafka Connect API | http://localhost:8083/connectors |

---

## Troubleshooting

**Gold views are empty**
```bash
psql -U postgres -d ecommerce_db -c "SELECT COUNT(*) FROM silver_orders WHERE order_purchase_timestamp IS NOT NULL;"
```
If count is 0, timestamps failed to parse. Clear checkpoints and rebuild:
```bash
task clean-checkpoints
docker compose up -d --build streaming-app
```

**Debezium connector in FAILED state**
```bash
task connector   # check status
curl -X POST http://localhost:8083/connectors/postgres-connector/restart
```

**Kafka broker conflict on restart**
```bash
task clean       # wipes volumes
task up
```

**Inspect a raw Kafka message**
```bash
docker exec ecommerce-dashboard-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic kafka_topic_prefix.public.orders \
  --from-beginning --max-messages 1
```

---

## Project Structure

```
ecommerce-dashboard/
├── csvs/                          # Source CSV files (Kaggle dataset)
├── debezium/
│   └── postgres-connector.json    # Debezium connector config 
|
├── dockerfiles/
│   ├── csv_to_db.Dockerfile
│   └── spark_app.Dockerfile
├── scripts/
│   ├── csv_to_db.py               # CSV loader
│   └── spark_app.py               # Spark streaming job
|
├── init.sql                       # Silver layer schema
├── gold_layer.sql                 # Gold materialized views
├── compose.yml                    # Main services
├── load-compose.yml               # CSV loader service
├── Taskfile.yml
└── .env.example
```

---

## Notes

- Debezium sends PostgreSQL timestamps as **microseconds since epoch** (`MicroTimestamp`). Spark converts them with `from_unixtime(col / 1000000)`.
- Clearing checkpoints causes Spark to re-read Kafka from the beginning. Upsert logic prevents duplicate data.
- Gold views refresh automatically when `orders` or `order_payments` batches are processed. Use `task refresh-gold` to trigger manually.
- `host.docker.internal` is a Docker DNS name for the host machine. Not available on Linux by default — use `172.17.0.1` or add `extra_hosts` to `compose.yml`.
