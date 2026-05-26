# 🛒 E-Commerce Clickstream Analytics — Real-Time Streaming Data Pipeline

## 📝 Problem Statement

In modern e-commerce, user behavior data (clickstreams) is generated continuously at high volumes. Processing this data in real-time is critical for understanding customer journeys, tracking revenue trends, and detecting system anomalies before they become business problems.

**The Engineering Challenge:**
Build a production-grade, fault-tolerant streaming pipeline that can ingest high-throughput clickstream events, process them reliably with schema enforcement, recover gracefully from bad data, and serve analytical insights through an automated batch transformation layer — all running reproducibly in a containerized local environment.

**Key Engineering Objectives:**
- ✅ **Schema-Enforced Event Ingestion**: Produce clickstream events with Avro serialization validated against a Schema Registry (Confluent-compatible, Redpanda built-in), preventing schema drift from breaking downstream consumers.
- ✅ **Real-Time Stream Processing**: Consume, deduplicate, and route events continuously using **Apache Flink (PyFlink 2.2)** with watermark-based event-time processing.
- ✅ **Dead Letter Queue (DLQ)**: Automatically isolate malformed or incomplete events (null `user_id` / `cart_total`) into a separate S3 DLQ partition, ensuring the main pipeline never silently loses data.
- ✅ **Chaos Engineering**: Deliberately inject 1-2% bad data (null fields, type errors) into the stream producer to validate fault-tolerance in a realistic scenario.
- ✅ **Data Lake Integration**: Sink validated events reliably into **AWS S3** in partitioned **Parquet** format with Snappy compression, via Flink's S3 filesystem connector.
- ✅ **Automated ELT Batch Workflow**: Orchestrate dbt transformations every 30 minutes using **Apache Airflow**, applying Medallion Architecture on top of S3.
- ✅ **Analytics Engineering**: Implement Bronze → Silver → Gold layers with data quality testing, SCD Type 2 dimension tracking, and incremental fact models using **dbt + DuckDB**.
- ✅ **Infrastructure as Code**: Fully reproducible AWS S3 provisioning with S3 versioning, lifecycle policies (STANDARD_IA → GLACIER → expiration), and remote Terraform state via **Terraform**.
- ✅ **CI/CD Automation**: Full CI pipeline (Ruff lint, SQLFluff, dbt compile, Terraform validate, Docker build, pytest) and CD pipeline (automated `terraform apply`) via **GitHub Actions**.

**Business Analytics Delivered:**
- **Traffic Analysis**: Active user sessions and page view trends across devices and UTM sources.
- **Conversion Funnel**: Per-UTM-source funnel from `page_view` → `view_item` → `add_to_cart` → `begin_checkout` → `purchase`.
- **Revenue Tracking**: 1-minute tumbling window revenue aggregations per user, sinked to S3 in near real-time.
- **Data Quality Visibility**: DLQ events surfaced and queryable separately from clean data.

---

## 🏗️ Architecture Overview

The pipeline implements a **Hybrid Streaming + Batch (Lambda-like) Architecture**:

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                              DATA PIPELINE FLOW                                       │
└──────────────────────────────────────────────────────────────────────────────────────┘

 [1. EVENT GENERATION]             [2. MESSAGE BROKER]          [3. STREAM PROCESSING]
┌─────────────────────┐          ┌──────────────────────┐     ┌────────────────────────┐
│ Python Mock Script  │          │ Redpanda (Kafka API) │     │ Apache Flink (PyFlink) │
│ - State Machine     │─Avro──▶  │ - Schema Registry    │────▶│ - Watermark / EventTime│
│ - 100 Concurrent    │          │ - DLQ Topic          │     │ - Tumbling Windows     │
│   User Sessions     │          │ - Redpanda Console   │     │ - DLQ routing          │
│ - Chaos Injection   │          └──────────────────────┘     └────────────┬───────────┘
└─────────────────────┘                                                     │
                                                                            ▼
                                                               [4. DATA LAKE — AWS S3]
                                                              ┌─────────────────────────┐
                                                              │ /raw/          (Parquet) │
                                                              │ /raw/total_revenue/      │
                                                              │ /dlq/          (Parquet) │
                                                              │ Partitioned by event_date│
                                                              └────────────┬────────────┘
                                                                           │
                                                          [5. BATCH TRANSFORMATION — every 30min]
                                                              ┌────────────▼────────────┐
                                                              │ dbt + DuckDB            │
                                                              │ - Staging (Silver)       │
                                                              │ - Snapshots (SCD Type 2)│
                                                              │ - Mart (Gold, Incremental)│
                                                              │ - Data Quality Tests     │
                                                              └────────────┬────────────┘
                                                                           │
                          ┌──────────────────────┐          ┌─────────────▼────────────┐
                          │ Apache Airflow        │          │ DuckDB File              │
                          │ (Orchestrates dbt,    │─────────▶│ ecommerce_data.duckdb    │
                          │  every 30 minutes)    │          └─────────────┬────────────┘
                          └──────────────────────┘                        │
                                                              ┌────────────▼────────────┐
                                                              │ Metabase Dashboard       │
                                                              │ - Conversion Funnel      │
                                                              │ - Revenue Analytics      │
                                                              └─────────────────────────┘

┌──────────────────────────────────┐   ┌────────────────────────────────────────────────┐
│ INFRASTRUCTURE (Terraform)       │   │ CI/CD (GitHub Actions)                         │
│ - AWS S3 (Data Lake + DLQ)       │   │ CI: Ruff, SQLFluff, dbt compile,               │
│ - S3 Versioning + Lifecycle      │   │     Terraform validate, Docker build, pytest    │
│ - AWS Glue Catalog               │   │ CD: terraform apply (auto-approve on main push) │
│ - Remote Terraform State (S3)    │   └────────────────────────────────────────────────┘
└──────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Event Streaming** | Redpanda v26.1 (Kafka-compatible) | High-performance message broker with built-in Schema Registry |
| **Schema Management** | Confluent Schema Registry (Redpanda built-in) | Avro schema enforcement, preventing schema drift |
| **Stream Processing** | Apache Flink 2.2.0 (PyFlink) | Real-time event-time processing, DLQ routing, windowed aggregations |
| **Data Lake** | AWS S3 | Scalable, partitioned storage for raw events and DLQ |
| **Infrastructure as Code** | Terraform + S3 Remote State | Reproducible AWS resource provisioning with lifecycle management |
| **Orchestration** | Apache Airflow 2.9.0 | Scheduling and managing 30-minute dbt batch cycles |
| **Analytics Engine** | DuckDB | Blazing-fast in-process OLAP for `read_parquet()` directly from S3 |
| **Data Transformation** | dbt 1.10.0 + dbt-utils | SQL-based Medallion Architecture with SCD Type 2 and incremental models |
| **Visualization** | Metabase + DuckDB Plugin | Interactive BI dashboard connected directly to DuckDB |
| **Stream Monitoring** | Redpanda Console | Real-time topic inspection, consumer lag, schema browser |
| **CI/CD** | GitHub Actions | Automated lint, test, build, and Terraform deployment |
| **Code Quality** | Ruff (Python) + SQLFluff (SQL) | Enforced code standards across Python and dbt models |
| **Testing** | pytest | Unit tests for event generator state machine |
| **Package Management** | uv | Ultra-fast, reproducible Python dependency management |
| **Containerization** | Docker Compose (10 services) | Fully reproducible local deployment |

---

## 🔬 Key Engineering Decisions

### 1. Avro + Schema Registry over plain JSON
Plain JSON has no enforcement — a producer can add/remove fields silently, breaking Flink's DDL at runtime. By using **Avro serialization** with Redpanda's built-in Schema Registry:
- Schema compatibility is checked at produce time (producer fails fast on violation)
- Flink uses `avro-confluent` format, fetching the schema automatically
- Null-union types (`["null", "string"]`) are declared explicitly in the schema, making nullable fields a first-class contract

### 2. Dead Letter Queue (DLQ) in the Flink STATEMENT SET
Rather than dropping bad events silently, the Flink job uses a single `EXECUTE STATEMENT SET` with three concurrent INSERTs:
- **Clean events** → `s3a://bucket/raw/` (Parquet, partitioned by `event_date`)
- **Purchase revenue window** → `s3a://bucket/raw/total_revenue/` (1-minute Tumbling window)
- **Bad events** (null `user_id` or `cart_total`) → `s3a://bucket/dlq/` (Parquet, queryable separately)

This ensures **zero data loss** — every event lands somewhere inspectable.

### 3. Chaos Engineering in the Producer
`mock_data.py` deliberately injects 1-2% malformed payloads (`user_id = None`, `cart_total = "error"`) to validate the DLQ routing works correctly in practice, not just in theory.

### 4. Medallion Architecture with DuckDB reading directly from S3
DuckDB's `read_parquet('s3://...', hive_partitioning=true)` capability allows the dbt staging layer to query S3 Parquet directly without a separate data warehouse service. Combined with Airflow running every 30 minutes, this achieves near-real-time analytics without the complexity of a distributed warehouse.

### 5. SCD Type 2 via dbt Snapshots
User profiles and product catalogs are tracked over time using dbt's `snapshot` with `strategy = 'check'`. Each time a user's `device_category`, `os_browser`, or `utm_source` changes, a new version is created with `dbt_valid_from` / `dbt_valid_to` timestamps. `mart_conversion_funnel` correctly filters `WHERE dbt_valid_to IS NULL` to use only current records.

### 6. Remote Terraform State on S3
Instead of committing `terraform.tfstate` to Git (a security risk), the project uses an S3 backend
This prevents credentials from leaking and enables team collaboration on infrastructure.

---

## 📁 Project Structure

```
e_commerce_clickstream/
├── .github/
│   └── workflows/
│       ├── ci.yml                     # CI: lint, test, validate on every PR
│       └── cd.yml                     # CD: terraform apply on main push
│
├── dags/
│   └── dbt_pipeline.py                # Airflow DAG (30-min schedule, 7 ordered tasks)
│
├── ecommerce_dbt/                     # dbt project (Medallion Architecture)
│   ├── models/
│   │   ├── staging/                   # Silver layer: deduplicate, cast, clean
│   │   │   ├── stg_clickstream.sql    # ROW_NUMBER dedup + NULLIF normalization
│   │   │   ├── stg_total_revenue.sql  # Revenue staging from Flink window output
│   │   │   ├── sources.yml            # S3 external source via read_parquet()
│   │   │   └── staging.yml            # Data quality tests (not_null, unique)
│   │   └── mart/                      # Gold layer: business-ready tables
│   │       ├── fact_events.sql        # Incremental fact table (unique_key=event_id)
│   │       ├── fact_purchases.sql     # Incremental purchases (event_name='purchase')
│   │       ├── mart_conversion_funnel.sql # UTM-source funnel with SCD2 join
│   │       └── mart.yml               # FK relationship tests (dim_products ref)
│   ├── snapshots/
│   │   ├── dim_user_snapshot.sql      # SCD Type 2: user profile history
│   │   ├── dim_products_snapshot.sql  # SCD Type 2: product catalog history
│   │   └── dim.yml                    # Tests: not_null, price >= 0
│   ├── profiles.yml                   # dev (local) + prod (Docker path) targets
│   └── packages.yml                   # dbt-utils 1.3.3
│
├── infra/
│   ├── main.tf                        # S3 data lake + versioning + lifecycle + Glue
│   └── variables.tf                   # Region, project name, credentials
│
├── scripts/
│   └── mock_data.py                   # Event generator: state machine + chaos injection
│
├── src/
│   └── job/
│       ├── store_data_s3.py           # PyFlink job: Avro source, 3-sink STATEMENT SET, S3-backed checkpoints
│       └── savepoints.sh              # Helper script: trigger savepoint → safe job upgrade
│
├── tests/
│   └── test_mock_data.py              # pytest: 6 unit tests for state machine logic
│
├── Dockerfile                         # Airflow image (+ dbt-duckdb + awscli)
├── Dockerfile.flink                   # PyFlink 2.2 image (Avro, Kafka, S3, Parquet JARs)
├── Dockerfile.metabase                # Metabase on eclipse-temurin:21-jre-jammy (glibc fix)
├── docker-compose.yml                 # 10 services: Redpanda, Console, Flink, Airflow, Metabase
├── flink-config.yaml                  # Custom Flink config (JVM metaspace for PyFlink)
├── redpanda-console-config.yaml       # Schema Registry + Admin API config for Console
├── .sqlfluff                          # SQLFluff: DuckDB dialect, dbt templater
├── pyproject.toml                     # Python deps (uv): ruff, sqlfluff, pytest, confluent-kafka
└── .env.example                       # Environment variables template
```

---

## 🔄 Pipeline Deep Dive

### Stage 1 — Event Generation (`scripts/mock_data.py`)

The mock data generator simulates realistic e-commerce traffic using a **probabilistic state machine**. 100 concurrent `UserSession` objects cycle through the conversion funnel:

```
page_view ──(75%)──▶ view_item ──(20%)──▶ add_to_cart ──(40%)──▶ begin_checkout ──(20%)──▶ purchase
    │                    │ (50%)               │ (30%)                 │ (80%)
    │(25%)               ▼                     ▼                       ▼
    └──────────────▶   [re-browse]           [drop]                  [drop]
                         │ (30%)
                         ▼
                       [drop]
```

Each session generates a complete event payload including session context (device, UTM source, IP), product info (id, category, price), and cart state. Events are serialized as **Avro** using the Confluent Schema Registry client and produced to Redpanda.

**Chaos Injection**: 1% of events have `user_id = None`, 1% have `cart_total = "error"` — these are caught by Flink and routed to DLQ.

**Fault handling**: Producer errors are logged to `failed_events.log` without crashing the generator.

### Stage 2 — Stream Broker (Redpanda)

Redpanda provides a Kafka-compatible API with two critical additions:
- **Schema Registry** (port 18081): Validates every Avro message against the registered `ClickstreamEvent` schema before accepting it
- **Redpanda Console** (port 8083): Web UI for inspecting topic messages, consumer group lag, and registered schemas in real-time

Two topics are created on startup via `redpanda-init`:
- `ecommerce_clickstream` — main event stream
- `clickstream_dlq` — dead letter queue topic (referenced in Flink DDL for bad-event routing)

### Stage 3 — Stream Processing (PyFlink `store_data_s3.py`)

The Flink job runs with event-time semantics and a **20-second watermark** to handle late-arriving events:

```python
WATERMARK FOR ts AS ts - INTERVAL '20' SECOND
```

A single `EXECUTE STATEMENT SET` runs **3 concurrent INSERT pipelines** from one source:

| Pipeline | Condition | Destination |
|----------|-----------|-------------|
| Raw events | `user_id IS NOT NULL AND cart_total IS NOT NULL` | `s3a://bucket/raw/` (Parquet) |
| Revenue windows | `event_name = 'purchase'`, 1-min Tumbling window | `s3a://bucket/raw/total_revenue/` |
| DLQ events | `user_id IS NULL OR cart_total IS NULL` | `s3a://bucket/dlq/` (Parquet) |

**Checkpointing**: Every 10 seconds ensures exactly-once S3 writes.  
**Parallelism**: Configurable via `FLINK_PARALLELISM` env variable (default: 1).

### Stage 4 — Batch Transformation (dbt + Airflow)

Airflow triggers the dbt pipeline every 30 minutes with 7 ordered tasks:

```
dbt_deps → dbt_run_staging → dbt_test_staging → dbt_snapshot → dbt_test_dim → dbt_run_mart → dbt_test_mart
```

**Medallion layers:**

| Layer | Models | Strategy | Key Logic |
|-------|--------|----------|-----------|
| **Silver (Staging)** | `stg_clickstream`, `stg_total_revenue` | View | `ROW_NUMBER()` dedup, `NULLIF()` normalization, type casting |
| **Dimensions (Snapshots)** | `dim_user`, `dim_products` | SCD Type 2 (`check`) | Tracks profile changes with `dbt_valid_from/to` timestamps |
| **Gold (Mart)** | `fact_events`, `fact_purchases` | Incremental (`unique_key=event_id`) | Appends only new events using `MAX(event_time)` watermark |
| **Gold (Mart)** | `mart_conversion_funnel` | Table | Per-UTM funnel rates, joins `dim_user WHERE dbt_valid_to IS NULL` |

### Orchestration Design Decision: BashOperator vs. Astronomer Cosmos

Two approaches were evaluated and **benchmarked** for running dbt inside Airflow:

| Approach | DAG Run Time | Trade-offs |
|----------|-------------|------------|
| **Astronomer Cosmos** (`DbtTaskGroup`) | ~10 minutes | Each dbt model becomes an individual Airflow task with full per-model observability, retries, and dependency graph in the Airflow UI. High overhead from task instantiation and dbt compilation per task. |
| **BashOperator** (`dbt run` per stage) | ~5 minutes | dbt manages its own dependency graph internally. One task per dbt stage, not per model — simpler and 2x faster at this pipeline's scale. |

**Decision: BashOperator** was chosen for a **2x speed improvement** (~5 min vs ~10 min per 30-minute cycle). Since this pipeline runs every 30 minutes to serve near-real-time analytics, DAG execution latency directly impacts data freshness. Cutting the transformation time in half means fresher data for the downstream Metabase dashboard.

The full Cosmos implementation is preserved as commented code in `dags/dbt_pipeline.py` and can be re-enabled when the model count grows large enough that per-model Airflow observability outweighs the startup overhead.

---

## ⚙️ CI/CD Pipeline

### Continuous Integration (on every Push/PR to `main`)
```
Checkout → Configure AWS → Install uv → Install deps
    → Ruff (Python lint)
    → dbt deps + SQLFluff (SQL lint, DuckDB dialect)
    → Docker Compose build
    → Terraform init + validate
    → dbt parse + compile (syntax check)
    → pytest tests/test_mock_data.py
```

### Continuous Deployment (on Push to `main`)
```
Checkout → Configure AWS → Terraform init → terraform apply -auto-approve
```

---

## 🚀 Quick Start Guide

### Prerequisites
- Python 3.12+ and `uv` ([install](https://astral.sh/uv/))
- Docker & Docker Compose
- AWS Account (S3 access) + Terraform installed

### Step 1: Clone & Configure

```bash
git clone https://github.com/hdminh279/e_commerce_clickstream_de_project.git
cd e_commerce_clickstream_de_project

# Copy env template and fill in your AWS credentials
cp .env.example .env

# Install Python dependencies
uv sync
source .venv/bin/activate
```

The `.env` file requires:
```env
AIRFLOW_UID=1000
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=ap-southeast-1
TARGET_S3_BUCKET=e-commerce-clickstream-data-lake
FLINK_PARALLELISM=1
```

### Step 2: Provision AWS Infrastructure

```bash
cd infra
terraform init   # Connects to S3 remote state backend
terraform plan
terraform apply
cd ..
```

This creates:
- S3 data lake bucket with versioning enabled
- S3 lifecycle: STANDARD_IA after 30d → GLACIER after 90d → expire after 750d
- S3 Athena results bucket
- AWS Glue Catalog database

### Step 3: Start All Services

```bash
docker compose up -d --build
```

Wait 1-2 minutes for Airflow and Flink to initialize. 10 services will start:
`redpanda`, `redpanda-init`, `redpanda-console`, `postgres`, `jobmanager`, `taskmanager`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`, `metabase`

### Step 4: Submit the Flink Streaming Job

```bash
docker compose exec jobmanager flink run -py /opt/src/job/store_data_s3.py
```

Verify the job is running at **Flink UI → http://localhost:8081**

### Step 5: Start Generating Events

```bash
uv run scripts/mock_data.py
```

You will see output like:
```
Send: 1000 events...
Error data! Write into failed_events.log, pipeline continue run
Send: 2000 events...
```

Monitor events in **Redpanda Console → http://localhost:8083**

### Step 6: Enable Airflow Batch Pipeline

- Access **Airflow UI → http://localhost:8080** (user: `airflow` / pass: `airflow`)
- Enable the `dbt_clickstream` DAG
- It runs every 30 minutes automatically, or trigger manually for immediate execution

---

## 🌐 Service Access

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | airflow / airflow |
| **Flink Web UI** | http://localhost:8081 | — |
| **Redpanda Console** | http://localhost:8083 | — |
| **Metabase** | http://localhost:3000 | Setup on first access |
| **Redpanda Schema Registry** | http://localhost:18081 | — |

---

## 📊 Metabase Dashboard Setup

1. Access Metabase at `http://localhost:3000` and create an admin account.
2. Add Database:
   - Type: **DuckDB**
   - Database Path: `/ecommerce_dbt/ecommerce_data.duckdb`
   - Advanced options → append `&read_only=true` (prevents file lock conflicts with Airflow)
3. Explore tables: `fact_purchases`, `fact_events`, `mart_conversion_funnel`, `dim_user`, `dim_products`

---

## 🧪 Testing

### Unit Tests (pytest)
```bash
uv run pytest tests/test_mock_data.py -v
```

6 tests covering the state machine:
- `test_initial_state` — correct defaults on UserSession creation
- `test_payload_excludes_internal_fields` — internal state never leaks to Kafka
- `test_cart_total` — cart total increments correctly on `add_to_cart`
- `test_drop_deactivates_session` — session deactivates and returns None after drop
- `test_purchase_creates_transaction_id` — TXN- prefix present only on purchase events
- `test_state_advances_after_event` — state machine transitions correctly (mocked)

### dbt Data Quality Tests
```bash
cd ecommerce_dbt
dbt test --target dev
```

Tests enforced:
- `stg_clickstream.event_id`: not_null + unique (catches duplicate events)
- `stg_clickstream.session_id`: not_null (severity: error)
- `fact_purchases.event_id`: not_null + unique
- `fact_purchases.product_id`: referential integrity → `dim_products.product_id`
- `dim_products.price`: `>= 0` via `dbt_utils.expression_is_true`

---

## 🐛 Error Handling & Engineering Lessons

| Challenge | Solution |
|-----------|----------|
| **Silent schema breaks** | Replaced plain JSON with Avro + Schema Registry — producer fails fast at schema mismatch |
| **Lost bad events** | Implemented DLQ in Flink STATEMENT SET — bad events land in `s3://bucket/dlq/` |
| **DuckDB file lock** (`PID 0` error with Metabase) | Resolved via `read_only=true` in connection + Ubuntu/glibc base image in `Dockerfile.metabase` |
| **S3 connectivity in Flink** | Deployed `flink-s3-fs-hadoop` to `/opt/flink/plugins/` (NOT `/opt/flink/lib/`) to avoid classpath conflicts |
| **Terraform state in Git** | Migrated to S3 remote backend — state now stored in `minh-terraform-state-bucket-2026` |
| **PyFlink JVM metaspace OOM** | Added `taskmanager.memory.jvm-metaspace.size: 512m` in `flink-config.yaml` |
| **SCD Type 2 stale join** | Added `WHERE dbt_valid_to IS NULL` in `mart_conversion_funnel` CTE |
| **In-memory checkpoints lost on restart** | Learned the difference between checkpoint (auto, periodic) vs savepoint (manual, on-demand). Migrated both to S3-backed storage with `RETAIN_ON_CANCELLATION` — job now resumes from last committed offset instead of replaying the full topic |
| **Unsafe Flink code upgrades** | Implemented `savepoints.sh` to snapshot job state before deploying new code, then resume with `-s <savepoint-path>` — zero data loss on upgrade |
| **Silent DAG failures** | Added `email_on_failure=True` + SMTP config via env variables — team gets notified immediately after 1 failed retry |

---

## 📦 Dependencies


Key dependencies managed via `uv` (see `pyproject.toml`):

| Package | Version | Purpose |
|---------|---------|---------|
| `apache-airflow` | 2.9.0 | Pipeline orchestration |
| `apache-flink` | 2.2.0 | Stream processing runtime |
| `confluent-kafka` | ≥2.14.0 | Kafka producer + Schema Registry client |
| `dbt-duckdb` | ≥1.10.1 | dbt adapter for DuckDB |
| `astronomer-cosmos` | ≥1.14.2 | dbt-Airflow integration (configured, optional) |
| `ruff` | ≥0.15.13 | Python linter/formatter |
| `sqlfluff` | ≥4.2.1 | SQL linter (DuckDB dialect + dbt templater) |
| `pytest` | ≥9.0.3 | Unit testing framework |
| `faker` | ≥40.11.1 | Realistic fake data for mock producer |
