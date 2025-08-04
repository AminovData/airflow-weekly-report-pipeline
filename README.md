# 🌀 Airflow Weekly Report Pipeline

This project demonstrates an **end-to-end ELT pipeline** using **Apache Airflow**, **Google Cloud Storage (GCS)**, **BigQuery**, **dbt**, and **Looker Studio**. It is designed as a **portfolio project** to showcase practical data engineering skills including orchestration, data validation, transformation, and dashboard reporting.

---

## 📁 Project Structure

```
airflow-weekly-report-pipeline/
│
├── dags/
│   └── weekly_report_pipeline.py          # Airflow DAG for weekly pipeline
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       └── weekly_aggregates.sql         # dbt model
│
├── scripts/
│   └── create_schema_and_seed_data.sql   # SQL to create schema and simulate data
│
├── .env                                   # Environment variables (not committed)
├── .gitignore                             # Ignore credentials & virtual envs
├── requirements.txt                       # Python dependencies
├── README.md                              # Project documentation (you are here)
└── docs/
    ├── airflow_pipeline_diagram.png      # [📸 Add Airflow DAG screenshot here]
    ├── transactional_schema.png          # [📸 Add PostgreSQL transactional schema diagram here]
    └── looker_studio_dashboard.png       # [📸 Add Looker Studio dashboard screenshot here]
```

---

## 🧠 Overview

This pipeline simulates an e-commerce platform where customer, order, and product data is processed weekly.

**Flow:**

1. **PostgreSQL** → Extract transactional data
2. **Soda SQL** → Run data quality scan
3. **Google Cloud Storage (GCS)** → Save raw data as CSV
4. **BigQuery** → Load cleaned CSV into staging table
5. **dbt** → Run transformations and create aggregates
6. **Looker Studio** → Visualize key KPIs and metrics

---

## 🔧 Technologies Used

* **Apache Airflow** (orchestration)
* **Google Cloud Platform**: GCS & BigQuery
* **PostgreSQL** (source system)
* **dbt** (transformations)
* **Soda SQL** (data quality checks)
* **Looker Studio** (dashboarding)
* **Slack Webhooks** (alerting)

---

## 🚀 Pipeline Tasks Breakdown

| Task                       | Description                                                   |
| -------------------------- | ------------------------------------------------------------- |
| `soda_raw_data_check`      | Validates source data using Soda SQL                          |
| `export_postgres_to_gcs`   | Queries & saves data from PostgreSQL to GCS in CSV format     |
| `load_csv_to_bigquery`     | Loads the CSV from GCS to BigQuery staging table              |
| `dbt_run_weekly_aggregate` | Transforms staging data using dbt into analytics-ready tables |

---

## 📊 Dashboard (Looker Studio)

* \[📸 Add screenshot: `docs/looker_studio_dashboard.png` here]
* Metrics include: Weekly Revenue, Customer Count, Product Popularity

---

## 🛡️ PostgreSQL Schema

* The schema is hosted in **PostgreSQL**, representing a **normalized OLTP transactional model**.

* Core tables:

  * `customers`, `products`, `orders`, `order_details`, `addons_purchases`

* Designed for referential integrity and realistic transactional workloads.

> 📸 ***Add schema screenshot: `docs/transactional_schema.png` here***

---

## 💩 Data Cleaning & Preparation

Before ingestion into the pipeline, data was manually simulated and engineered:

* A **PostgreSQL database** was created from scratch using SQL DDL.
* Tables: `customers`, `products`, `orders`, `order_details`, `addons_purchases`
* Columns added to enrich complexity and analytical power:

  * `loyalty_member`, `gender`, `registration_date`, `addon_total`, `rating`, etc.
* Data types, enums (e.g., `order_status_enum`), and constraints were defined for integrity.
* Scripts inserted **synthetic but realistic data** using Python and SQL logic.

> ✅ See [`scripts/create_schema_and_seed_data.sql`](scripts/create_schema_and_seed_data.sql) for the full schema setup and generation.

> 📸 ***Add before/after snapshots of sample data if desired***

---

## 🗂️ Airflow DAG Overview

* Runs weekly (customizable via `schedule_interval`)
* Includes Slack alerting on failure

> 📸 ***Add Airflow DAG screenshot: `docs/airflow_pipeline_diagram.png` here***

---

## 📝 Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/airflow-weekly-report-pipeline.git
cd airflow-weekly-report-pipeline
```

### 2. Set Up Environment Variables

Create a `.env` file at the root:

```env
DBT_PATH=/absolute/path/to/dbt-project
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your-service-account.json
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
SODA_CONFIG_FILE=/path/to/soda/config.yml
SODA_CHECK_FILE=/path/to/soda/check.yml
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Run Airflow

```bash
airflow db init
airflow webserver --port 8080
airflow scheduler
```

### 5. Trigger DAG

Via Airflow UI or CLI:

```bash
airflow dags trigger weekly_report_pipeline
```

---

## 📦 Future Improvements

* Add automated dbt testing + documentation
* Add unit/integration testing to DAG logic
* Parametrize for multi-region processing

---

## 👨‍💻 Author

**Alidzhon Aminov**
Data Engineer & Data Scientist

---

## 📟 License

This project is for educational purposes only.
