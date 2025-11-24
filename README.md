# DVD Rental ELT Pipeline

Automated ELT (Extract, Load, Transform) pipeline that syncs data from a PostgreSQL database to Databricks using Airbyte, then transforms it with dbt to create a star schema optimized for analytics.

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| **Orchestration** | GitHub Actions |
| **Data Integration** | Airbyte |
| **Data Warehouse** | Databricks |
| **Transformation** | dbt |
| **Containerization** | Docker |
| **Language** | Python 3.13, SQL |

---

## Project Overview

This project automates the complete data pipeline for a DVD rental business:

1. **Extract & Load**: Airbyte syncs 22 tables from PostgreSQL to Databricks
2. **Transform**: dbt transforms raw data into a star schema with dimensions and facts
3. **Automate**: GitHub Actions runs the pipeline daily (or on-demand)

### Data Architecture

```
PostgreSQL (Source)
    ↓ Airbyte
Databricks (Raw Tables)
    ↓ dbt
Star Schema:
├── Dimensions: dim_customer, dim_film, dim_store, dim_date
├── Facts: fct_rental
└── Analytics: rental_analytics (denormalized for BI)
```

---

## Quick Start

### Prerequisites

- **Airbyte** (self-hosted) with PostgreSQL source configured
- **Databricks** workspace with SQL warehouse
- **GitHub** account (for CI/CD)
- **Python 3.13+** (for local development)

### 1. Clone Repository

```bash
git clone https://github.com/ariannalangwang/elt-project.git
cd elt-project
```

### 2. Set Up GitHub Secrets

Go to **Settings → Secrets and variables → Actions** and add:

| Secret Name | Where to Find It |
|------------|------------------|
| `DATABRICKS_HOST` | Databricks workspace URL (e.g., `dbc-xxx.cloud.databricks.com`) |
| `DATABRICKS_HTTP_PATH` | SQL warehouse → Connection Details → HTTP Path |
| `DATABRICKS_TOKEN` | User Settings → Developer → Access Tokens |
| `AIRBYTE_URL` | Your Airbyte API URL (e.g., `http://localhost:8000/api`) |
| `AIRBYTE_CLIENT_ID` | Airbyte → Settings → Applications |
| `AIRBYTE_CLIENT_SECRET` | Airbyte → Settings → Applications |
| `AIRBYTE_WORKSPACE_ID` | Airbyte workspace URL (last part) |
| `AIRBYTE_CONNECTION_ID` | Airbyte → Connections → Connection ID |

**Total: 8 secrets required**

### 3. Push to GitHub

```bash
git add .
git commit -m "Initial setup"
git push origin main
```

**That's it!** The pipeline will:
- Build Docker image automatically
- Run daily at 2 AM UTC
- Be available for manual triggers anytime

---

## How to Run

### Automated Execution (Recommended)

**Daily at 2 AM UTC** - No action needed, pipeline runs automatically.

### Manual Execution

1. Go to your GitHub repository
2. Click **Actions** tab
3. Select **ELT Pipeline** workflow
4. Click **Run workflow** → Select `main` → **Run workflow**
5. Watch the execution in real-time


---

## Local Development

#### Set Up Environment

```bash
# Create .env file with your credentials
cp env.example .env
# Edit .env with your actual values

# Install dependencies
uv sync
```

#### Run Airbyte Ingestion Sync

```bash
# Load environment variables
set -a && source .env && set +a

# Trigger sync
python data_ingestion/trigger_sync.py YOUR_CONNECTION_ID
```

#### Run dbt Transformations

```bash
cd data_transformation/dvd_rental

# Test connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

---

## GitHub Actions Workflow

**Workflow** (`.github/workflows/elt-pipeline.yml`) handles everything:

### Jobs

1. **Build Docker** - Builds and pushes image to GitHub Container Registry
2. **Data Ingestion** - Triggers Airbyte sync from PostgreSQL → Databricks
3. **dbt Transform** - Runs dbt models, tests, and generates documentation
4. **Summary** - Reports pipeline execution status

### Triggers

- **Push to `main`**: Builds Docker image only (fast, ~3-5 min)
- **Daily at 2 AM UTC**: Runs full pipeline (complete, ~10-20 min)
- **Manual trigger**: Runs full pipeline anytime

### Execution Flow

```
Push to main
    ↓
Build Docker Image → GitHub Container Registry
    ↓
[IF schedule/manual trigger]
    ↓
Trigger Airbyte Sync → Wait for completion
    ↓
Run dbt (run → test → docs)
    ↓
Upload artifacts & Summary Report
```

---

## What Gets Built

After a successful pipeline run:

### Databricks Tables

| Layer | Object | Type | Description |
|-------|--------|------|-------------|
| **Staging** | `stg_*` (11 views) | View | Cleaned source data |
| **Dimensions** | `dim_customer` | Table | Customer details |
| | `dim_film` | Table | Film catalog |
| | `dim_store` | Table | Store information |
| | `dim_date` | Table | Date dimension |
| **Facts** | `fct_rental` | Table | Rental transactions |
| **Analytics** | `rental_analytics` | Table | Denormalized for BI tools |

**Total: 17 dbt models**

### Data Quality

**54 dbt tests** validate:
- Primary key uniqueness
- Foreign key relationships
- Not-null constraints
- Accepted value ranges

### Documentation

- dbt generates lineage graphs
- Column descriptions
- Test results
- Available as downloadable artifact

---

## Troubleshooting

### Pipeline Fails on GitHub Actions

**Check secrets are set:**
```bash
Settings → Secrets and variables → Actions
```
Verify all 8 secrets are present.

**View logs:**
```bash
Actions → Click workflow run → Click failed job → Expand steps
```

### Airbyte Sync Fails

**Verify connection ID:**
```bash
# Go to Airbyte UI
Connections → Your Connection → Copy ID from URL
```

**Check Airbyte is running:**
```bash
# If self-hosted
docker ps | grep airbyte
```

### dbt Connection Error

**Test Databricks connection:**
```bash
cd data_transformation/dvd_rental
dbt debug
```

**Verify credentials:**
- Check token hasn't expired
- Ensure SQL warehouse is running
- Confirm catalog/schema exist

### Local Development Issues

**Environment variables not loaded:**
```bash
# For bash/zsh
set -a && source .env && set +a

# Verify
echo $DATABRICKS_HOST
```

**Dependencies missing:**
```bash
# Install with UV (recommended)
uv sync

# Or install UV first if not installed
pip install uv
```

---

## Monitoring

### Pipeline Status

**GitHub Actions:**
- Green checkmark = Success
- Red X = Failure
- Yellow dot = In progress

**View metrics:**
```bash
Actions → Workflow runs → View timing and logs
```

### Data Quality

**dbt test results:**
```bash
# In dbt output or artifacts
54 tests passed ✓
```

**Check Databricks:**
```sql
-- Verify row counts
SELECT COUNT(*) FROM workspace.dvd_rental.fct_rental;
SELECT COUNT(*) FROM workspace.dvd_rental.dim_customer;
```

 
