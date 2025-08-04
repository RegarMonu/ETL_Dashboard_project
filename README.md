# Catalogue Performance Tracker ETL Pipeline

This project implements an ETL pipeline for tracking catalogue performance, integrating Google Drive data, Google Cloud Storage metadata, and syncing with a PostgreSQL analytics database. It supports incremental updates, metadata management, and materialized view refreshes for reporting.

## 📊 Demo Dashboards

Our project includes three main dashboards, each designed to provide actionable insights at different organizational levels:

### 1. Client Level Dashboard
![Dashboard 1](assets/clientlevel.jpg)
- **Purpose:** Shows KPIs and performance metrics for each client.
- **Insights:** Track work progress, completed tasks, and file uploads per client. Useful for client-specific reporting and identifying bottlenecks or high-performing clients.

### 2. Associate Level Dashboard
![Dashboard 2](assets/associatelevel.jpg)
- **Purpose:** Displays individual associate performance.
- **Insights:** Monitor workload, completion rates, and contributions by each associate. Helps in resource allocation and recognizing top performers.

### 3. Team Level Dashboard
![Dashboard 3](assets/teamlevel.jpg)
- **Purpose:** Aggregates data at the team level.
- **Insights:** Compare teams on overall output, efficiency, and file management. Useful for management to assess team productivity and balance workloads.

---

Each dashboard is powered by the ETL pipeline and refreshed materialized views, ensuring up-to-date analytics for informed decision-making. The dashboards include three dynamic filters based on client, associate, and team, providing flexible and easily manageable views.

## Features

- **Google Drive Integration:** Imports Excel data from Google Drive using service account credentials.
- **Google Cloud Storage Metadata Extraction:** Extracts and tracks file metadata from GCS buckets.
- **PostgreSQL Sync:** Upserts data into normalized tables (`work_in_progress`, `work_completed`, `file_tracker`, etc.).
- **Fact Table & Dimension Management:** Maintains fact and dimension tables for analytics.
- **Materialized View Refresh:** Refreshes reporting views for up-to-date KPIs.
- **Logging:** Rotating logs for all ETL steps.
- **Configurable via `.env` and JSON credentials.**

## Project Structure

    project-root/
    │
    ├── src/                          # Main source code
    │   ├── main.py                   # Entry point for the data pipeline
    │   ├── data_importer.py          # Handles importing data from Google Drive
    │   ├── folder_details_extraction.py  # Extracts metadata from GCS folders
    │   ├── db_exporter.py            # Inserts/updates data into the database
    │   ├── folder_db_exporter.py     # Updates file tracker in the database
    │   ├── client_associate_id_update.py # Syncs client associate IDs (dimension update)
    │   ├── wc_fact_table_insertion.py    # ETL logic for populating fact tables
    │   ├── refresh_materialized_view.py  # Refreshes materialized views in the DB
    │   └── utils/                    # Utility functions and shared modules
    │       ├── db.py                 # Database connection and helpers
    │       ├── logger.py            # Centralized logging setup
    │       └── etl_metadata.py      # ETL metadata tracking (timestamps, run history, etc.)
    │
    ├── SQL_query/
    │   └── FINAL_QUERY_TABLE.sql     # SQL schema and base queries for database setup
    │
    ├── config/                       # Configuration files
    │   └── service_account.json      # Google service account credentials
    │
    ├── artifacts/                    # Output artifacts (e.g., logs, exported CSVs)
    │   ├── logs/                     # Log files from pipeline runs
    │   └── exports/                  # Exported CSVs or reports
    │
    ├── requirements.txt              # Python package dependencies
    ├── .env                          # Environment-specific variables (e.g., secrets, configs)
    └── README.md                     # Project documentation


---

## 🚀 Setup

1. **Clone the repository:**

   ```sh
   git clone https://github.com/RegarMonu/ETL_Dashboard_project.git
   cd your-project-directory
    ```
2. **Install dependencies:**

    ```sh
    pip install -r requirements.txt
    ```
3. **Configure environment variables:**
    Create a .env file with your DB and GCS settings:
    ```sh
    DB_NAME=your_db
    DB_USER=your_user
    DB_PASSWD=your_password
    DB_HOST=your_host
    DB_PORT=5432
    BUCKET_NAME=your_bucket
    BUCKET_PATH=your_prefix
    BUCKET_CREDENTIALS_PATH=path/to/xyz.json
    CSV_PATH=artifacts/csv_exports
    ```

4. **Add Google service account JSONs to the config/ directory.**
5. **Set up the database:**

    Run the SQL in SQL_query/FINAL_QUERY_TABLE to create tables and views.

6. **Connect Database to Apache Superset for Dashboard Visuallization:**

    In Superset, navigate to **Data > Databases > + Database**, and use the following SQLAlchemy URI format:
    ```sh
    postgresql+psycopg2://<username>:<password>@<host>:<port>/<database>
    ```
    **Example:**
    ```sh
    postgresql+psycopg2://postgres:1234@127.0.0.1:5432/xyz_db
    ```

    **Make sure:**

        1. The database allows external connections.
        2. The user has necessary permissions (e.g., `SELECT`).
        3. Superset's environment has access to the host and port.

## 🛠️ Usage

To run the full pipeline from the root directory:

```sh
python3 -m src.main
```
---
    

## ✅ Requirements
    1. Python 3.8+
    2. PostgreSQL or compatible DB
    3. Access to a GCP bucket with the appropriate service account.
    4. Required Python packages (see requirements.txt)
    5. Apache Superset

## 📌 Notes

    1. Materialized views must exist in the DB before refreshing.
    2. GCP service account must have access to read from the bucket.
    3. This project uses dummy data for demonstration purposes.
While the dashboards reflect the actual project setup, the underlying data is synthetic to protect company confidentiality.