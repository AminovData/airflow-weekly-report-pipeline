# 🌀 Airflow Weekly Report Pipeline

This project demonstrates an **end-to-end ELT pipeline** using **Apache Airflow**, **Google Cloud Storage (GCS)**, **BigQuery**, **dbt**, and **Looker Studio**. 
---

## 📁 Project Structure

```
airflow-weekly-report-pipeline/
│
├── dags/
│   └── weekly_report_pipeline.py 
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       └── data_preparation.ipynb
│       └── data_insert_and_schema_creation.ipynb
│
├── data_preparation_files/
│   └── create_schema_and_seed_data.sql
│
├── .env                                   
├── .gitignore 
├── requirements.txt                      
├── README.md 
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

## 🔧 Used Tools

* **Apache Airflow** (orchestration)
* **Google Cloud Platform**: GCS & BigQuery
* **PostgreSQL** (source system)
* **dbt** (transformations)
* **Soda SQL** (data quality checks)
* **Looker Studio** (dashboarding)
* **Slack Webhooks** (alerting)

---

## 🚀 Pipeline Tasks

| Task                       | Description                                                   |
| -------------------------- | ------------------------------------------------------------- |
| `soda_raw_data_check`      | Validates source data using Soda SQL                          |
| `export_postgres_to_gcs`   | Queries & saves data from PostgreSQL to GCS in CSV format     |
| `load_csv_to_bigquery`     | Loads the CSV from GCS to BigQuery staging table              |
| `dbt_run_weekly_aggregate` | Transforms staging data using dbt into analytics-ready tables |

---

## 📊 Dashboard (Using Looker Studio)

<img width="804" height="602" alt="image" src="https://github.com/user-attachments/assets/165c1cd4-ebf0-4808-be4a-da3b82d7fc70" />
Link: https://lookerstudio.google.com/reporting/129394b1-1236-4f70-99fe-ea141b592aab
---

## 🛡️ PostgreSQL Schema

* The schema is hosted in **PostgreSQL**, representing a **normalized OLTP transactional model**.

* Core tables:

  * `customers`, `products`, `orders`, `order_details`, `addons_purchases`

* Designed for referential integrity and realistic transactional workloads.

<img width="971" height="576" alt="image" src="https://github.com/user-attachments/assets/f88b0f19-83bd-4ef4-b685-9832f85c5e2b" />
The {} file shows how PostgreSQL was created using Python + PostreSQL

---

## 💩 Data Cleaning & Preparation & Modification

Before ingestion into the pipeline, the raw data went through cleaning and modification process to reflect realistic business scenarios:

✅ Data Cleaning: Removed duplicates, handled null values, ensured consistent data types.

⚖️ Data Modification:  Adjusted customer activity, order frequency, and event-based sales spikes (e.g., holiday boosts) to reflect realistic trends. 

🧬 Synthetic Data Generation: Created additional columns using Python to add complexity and analytical depth:

    - Examples include loyalty_member, gender, registration_date, addon_total, rating, and more.
🔎 The {} file shows the all of the above process

---

## 🗂️ Airflow DAG Overview

* Runs weekly 

<img width="980" height="429" alt="image" src="https://github.com/user-attachments/assets/8561a34d-7d2a-4c25-80c7-18f7ddc6e2d9" />


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

## 👨‍💻 Author

**Alidzhon Aminov**
Data Engineer & Data Scientist

---
