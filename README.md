# 🛒 E-Commerce Clickstream Analytics - End-to-End Streaming Data Pipeline

## 📝 Problem Statement

In modern e-commerce, user behavior data (clickstreams) is generated continuously at high volumes. Processing this data in real-time or near real-time is critical for understanding customer journeys, tracking revenue, and identifying system anomalies. 

**The Challenge & Purpose:**
This project is designed as an advanced **Data Engineering sandbox**. By simulating a high-throughput e-commerce clickstream, the primary objective is to **deliberately encounter, handle, and recover from streaming pipeline errors** (e.g., late-arriving data, duplicates, schema evolution, and connection drops). It demonstrates a production-grade integration of modern streaming tools, data lakehouse concepts, and analytics engineering.

**Key Engineering Objectives:**
- ✅ **Real-time Event Generation**: Simulate continuous user traffic and purchase events (Python/Kafka).
- ✅ **Streaming Ingestion & Processing**: Consume, deduplicate, and process events continuously using **Apache Flink**.
- ✅ **Data Lake Integration**: Sink processed streaming data reliably into AWS S3.
- ✅ **Automated ELT Workflow**: Orchestrate batch transformations every 30 minutes using **Apache Airflow**.
- ✅ **Analytics Engineering**: Implement Medallion Architecture (Bronze, Silver, Gold) with strict data quality testing using **dbt** and **DuckDB**.
- ✅ **Infrastructure as Code**: Reproducible AWS S3 provisioning via **Terraform**.

**Business Analytics Delivered:**
- **Traffic Analysis**: Active user sessions and page view trends.
- **Conversion Funnel**: Tracking the user journey from viewing an item to checkout and purchase.
- **Revenue Tracking**: Aggregating real-time sales and monitoring daily revenue targets.

---

## 🏗️ Architecture Overview

The pipeline utilizes a hybrid **Streaming Ingestion + Batch Transformation** approach.

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   DATA PIPELINE FLOW                                   │
└────────────────────────────────────────────────────────────────────────────────────────┘

 [1. EVENT GENERATION]         [2. STREAM PROCESSING]             [3. DATA LAKE]
┌─────────────────────┐      ┌────────────────────────┐      ┌───────────────────────┐
│ Python Mock Script  │      │ Apache Flink (PyFlink) │      │ AWS S3 (Data Lake)    │
│ (User Clicks/Buys)  │───>  │ - Consume from Kafka   │───>  │ - Parquet Format      │
│                     │      │ - Deduplication        │      │ - Snappy Compression  │
└─────────────────────┘      └────────────────────────┘      └───────────────────────┘
          │                              ▲                               │
          v                              │                               v
┌─────────────────────┐                  │                 [4. BATCH TRANSFORMATION]
│ Redpanda (Kafka)    │──────────────────┘               ┌───────────────────────┐
│ (Message Broker)    │                                  │ dbt + DuckDB          │
└─────────────────────┘                                  │ - Medallion Arch      │
                                                         │ - Data Quality Tests  │
                                                         └───────────────────────┘
                                                                     │
                                                                     v
                             [5. VISUALIZATION]          ┌───────────────────────┐
┌─────────────────────┐    ┌────────────────────┐        │ Local Data Warehouse  │
│ Orchestration       │    │ Metabase           │ <───── │ ecommerce_data.duckdb │
│ Apache Airflow      │──> │ - Revenue Dash     │        └───────────────────────┘
│ (Schedules dbt)     │    │ - Traffic Trends   │ 
└─────────────────────┘    └────────────────────┘ 
```

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Event Streaming** | Redpanda (Kafka-compatible) | High-performance message broker |
| **Stream Processing** | Apache Flink (PyFlink) | Real-time data processing and deduplication |
| **Cloud Storage** | AWS S3 | Scalable Data Lake |
| **Infrastructure as Code** | Terraform | Provisioning AWS S3 buckets |
| **Orchestration** | Apache Airflow 2.9.0 | Scheduling and managing batch dbt transformations |
| **Analytics Engine** | DuckDB | Blazing fast, in-process analytical database |
| **Data Transformation** | dbt 1.10.0 | SQL-based transformations & data quality testing |
| **Visualization** | Metabase | Interactive BI Dashboards |
| **Environment Mgmt** | `uv` | Ultra-fast Python package management |
| **Containerization** | Docker Compose | Local reproducible deployment |

---

## 📁 Project Structure

```text
e_commerce_clickstream/
├── dags/                      # Airflow DAGs (dbt_pipeline.py)
├── ecommerce_dbt/             # dbt project (Medallion models: staging, snapshots, mart)
├── infra/                     # Terraform code for AWS resources
├── scripts/                   # Python scripts (mock_data.py to simulate traffic)
├── src/                       # PyFlink source code (store_data_s3.py)
├── metabase-plugins/          # Custom Metabase drivers (DuckDB)
├── .env.example               # Environment variables template
├── docker-compose.yml         # Container orchestration
├── Dockerfile                 # Custom Airflow image (includes dbt & DuckDB)
├── Dockerfile.flink           # Custom PyFlink image
├── Dockerfile.metabase        # Custom Metabase Ubuntu-based image for DuckDB support
└── uv.lock & pyproject.toml   # Python dependencies management
```

---

## 🚀 Quick Start Guide

### Prerequisites
- Python 3.11+ and `uv` package manager.
- Docker & Docker Compose.
- AWS Account (for S3 storage) and Terraform installed.

### Step 1: Clone & Configure Environment
```bash
git clone [https://github.com/hdminh279/e_commerce_clickstream_de_project.git](https://github.com/hdminh279/e_commerce_clickstream_de_project.git)
cd e_commerce_clickstream_de_project

# Copy env template and fill in your AWS credentials
cp .env.example .env

# Install dependencies using uv
uv sync
source .venv/bin/activate
```

### Step 2: Infrastructure Setup (AWS S3)
Provision the S3 bucket to act as your data lake:
```bash
cd infra
terraform init
terraform apply
cd ..
```

### Step 3: Start the Docker Infrastructure
Spin up Redpanda, Flink, Postgres, Airflow, and Metabase:
```bash
docker compose up -d --build
```
*Wait 1-2 minutes for Airflow and Flink to initialize.*

### Step 4: Run the Pipeline

**1. Start the Stream Processor (PyFlink):**
Submit the Flink job to consume from Redpanda and write to S3:
```bash
docker compose exec jobmanager flink run -py /opt/src/job/store_data_s3.py
```

**2. Generate Mock E-commerce Data:**
Open a new terminal and run the clickstream generator (this will feed events into Redpanda):
```bash
uv run scripts/mock_data.py
```

**3. Trigger the Batch Pipeline (Airflow):**
- Access Airflow UI at `http://localhost:8080` (User/Pass: `airflow` / `airflow`)
- Turn on the DAG: `dbt_clickstream`
- The DAG will run every 30 minutes to pull new Parquet files from S3, run dbt tests, update snapshots, and build the analytical marts in DuckDB.

---

## 📊 Analytics Visualization (Metabase)

1. Access Metabase at `http://localhost:3000`.
2. Create an admin account.
3. Connect the Database:
   - Type: **DuckDB**
   - Database Path: `/ecommerce_dbt/ecommerce_data.duckdb`
   - *Important:* In advanced options, append `&read_only=true` to connection options to avoid file locking conflicts with Airflow.
4. Explore your tables (`fact_purchases`, `stg_total_revenue`, etc.) and build your Dashboards!

---

## 🐛 Error Handling & Lessons Learned

As the main goal of this project was to simulate and solve streaming pipeline errors, here are key challenges addressed:
- **Duplicate Events:** Handled via PyFlink's stateful deduplication and dbt's `ROW_NUMBER()` logic.
- **DuckDB File Locks (`PID 0` error):** Resolved by implementing `read_only=true` in Metabase and correctly isolating the Ubuntu/glibc environment in a custom Metabase Dockerfile.
- **Schema Evolution:** Used dbt snapshots (`dim.yml`) to implement Slowly Changing Dimensions (SCD Type 2) for tracking user profile updates over time.
- **S3 Connectivity:** Configured Hadoop AWS libraries inside the Flink container to smoothly sink streaming Parquet files.