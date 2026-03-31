# ETL Pipeline for Network Performance KPI

This repository contains a robust Python-based ETL (Extract, Transform, Load) pipeline designed to automate the ingestion of Network Performance KPI and related data from Microsoft OneDrive into a PostgreSQL database.

## 🌟 Overview

The pipeline scripts fetch periodic CSV reports directly from a designated OneDrive folder via the **Microsoft Graph API**. It dynamically processes these reports, performs data cleansing, manages schema evolution, and utilizes high-performance bulk inserts (`COPY`) to load the data into a centralized **PostgreSQL** data warehouse.

This pipeline is intended to run as a scheduled automated task (cron job).

## ✨ Key Features

- **Automated Data Retrieval:** Integrates with Microsoft Graph API using App-Only (Client Credentials) authentication to securely download CSV files from OneDrive without user interaction.
- **Dynamic Schema Evolution:** Automatically detects new columns in incoming CSV files and alters the target PostgreSQL tables to add these columns as `TEXT`, ensuring the ingestion process never fails due to schema drift.
- **Smart Data Cleansing:** Standardizes different representations of missing data (e.g., `NaN`, `null`, `none`, empty strings, whitespace) into native PostgreSQL `NULL` values.
- **Idempotent Ingestion:** Implements mechanism to track successfully ingested files in a metadata table (`ingest_log_files`) to prevent duplicate data loading, even if the script is re-run.
- **High-Performance Load:** Utilizes PostgreSQL's `COPY` command via `io.StringIO` to perform extremely fast bulk inserts, significantly reducing ingestion time compared to standard row-by-row inserts.
- **Pagination Handling:** Safely handles folders with large numbers of files using Microsoft Graph API's `@odata.nextLink` pagination.
- **File Regex Filtering:** Each ingest script is targeted to its specific report type using regex patterns (e.g., matching `2G_daily_*.csv`).

## 🗂️ Project Structure

The directory contains several modular ingestion scripts, each responsible for a specific data domain, and a crontab definition:

- `import 2g.py` - Ingests 2G Daily KPI reports.
- `import busyhour.py` - Ingests Busy Hour reports.
- `import pl.py` & `import pl hourly.py` - Ingests Payload (PL) reports (Daily/Hourly).
- `import prb_max.py` - Ingests PRB Max reports.
- `import os new v2.py` - General OS/miscellaneous ingestion.
- `crontab.txt` - Provides the schedule configuration for running the ingestion jobs automatically using Linux `cron`.

## 🚀 Getting Started

### Prerequisites

- **Python 3.8+**
- **PostgreSQL** database
- **Azure Active Directory (AAD) App Registration** with `Files.Read.All` or similar permissions (Admin Consent required) for Microsoft Graph API access.

### Installation

1. Clone or download this repository to the host server.
2. Create a virtual environment and activate it:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
3. Install the required Python packages:
   ```bash
   pip install pandas requests msal sqlalchemy psycopg2-binary python-dotenv
   ```

### Configuration

Each script expects an environment variables file (`.env`). Ensure you have the corresponding `.env` files (e.g., `.env.2g`, `.env.bh`) configured in the path specified within the respective Python scripts.

**Example `.env` structure:**

```ini
# Microsoft Graph API Credentials
TENANT_ID=your-tenant-id
CLIENT_ID=your-client-id
CLIENT_SECRET=your-client-secret
USER_UPN=admin@yourdomain.onmicrosoft.com
ONEDRIVE_FOLDER_PATH=KPI

# PostgreSQL Database Credentials
PG_HOST=db.yourserver.com
PG_PORT=5432
PG_DB=your_database_name
PG_USER=your_db_user
PG_PASSWORD=your_db_password
PG_SCHEMA=public
PG_TABLE=target_table_name
```

## ⚙️ Automation (Cron)

To automate the pipeline, you can set up cron jobs based on the provided `crontab.txt`. Ensure the paths match your server's configuration.

Example `crontab` setup (`crontab -e`):

```cron
# 2G Daily Import at 08:05 AM
05 8 * * * cd /home/apps/etl-python && /home/apps/etl-python/venv/bin/python import_2g.py >> /home/apps/etl-python/logs/cron_2g.log 2>&1

# Busy Hour Import at 07:50 AM
50 7 * * * cd /home/apps/etl-python && /home/apps/etl-python/venv/bin/python import_busyhour.py >> /home/apps/etl-python/logs/cron_busyhour.log 2>&1
```

## 🛡️ Best Practices & Notes

- **Initial Load:** Upon the first execution, target tables and the `ingest_log_files` table will be automatically created in the specified PostgreSQL schema.
- **Monitoring:** Always pipe the cron output to a `.log` file to monitor the execution and troubleshoot any connectivity or data issues.
- **Data Types:** For reliable staging and dynamic schema updates, all ingested fields are mapped to `TEXT` type in PostgreSQL initially. Downstream views or transformations should cast them to appropriate types (e.g., `INT`, `FLOAT`, `TIMESTAMP`) inside the data warehouse.

---
*Built for resilient and scalable KPI data ingestion.*
