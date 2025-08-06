from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator



import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

import requests
import logging





load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

dbt_path = os.getenv("DBT_PATH")
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
soda_config_file = os.getenv("SODA_CONFIG_FILE")
soda_check_file = os.getenv("SODA_CHECK_FILE")
webhook_url_slack = os.getenv("SLACK_WEBHOOK_URL")


def notify_slack_on_failure(context):
    try:
        if not webhook_url_slack:
            logging.error("SLACK_WEBHOOK_URL environment variable not set!")
            return

        message = (
            f":red_circle: Task has failed :red_circle:.\n"
            f"DAG ID: {context['dag'].dag_id}\n"
            f"TASK ID: {context['task_instance'].task_id}\n"
            f"DATE OF EXECUTION: {context['execution_date']}\n"
            f"LOG URL: {context['task_instance'].log_url}"
        )

        payload = {"text": message}
        response = requests.post(webhook_url_slack, json=payload)

        if response.status_code != 200:
            logging.error(f"Slack notification failed: {response.text}")

    except:
        logging.error(f"Slack notification failed: {str(e)}")



default_args = {
    "owner": "aminovdata",
    "start_date": datetime(2025, 4, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),  
}


with DAG(
    dag_id="weekly_report_pipeline",
    default_args=default_args,
    schedule_interval="@weekly",
    catchup=False,
    tags=["elt ", "validation", "dashboard"],
    on_failure_callback=notify_slack_on_failure,
) as dag:

    soda_raw_check = BashOperator(
    task_id='soda_raw_data_check',
    bash_command= f'soda scan -c {soda_config_file} -d transaction_database {soda_check_file}',
    on_failure_callback=notify_slack_on_failure,
    )


    export_postgres_to_gcs = PostgresToGCSOperator(
    task_id="export_postgres_to_gcs",
    postgres_conn_id="postgres_product_el", 
    sql="""
        SELECT
            c.customer_id, 
            c.age, 
            c.gender, 
            c.loyalty_member, 
            c.registration_date,
            o.order_id, 
            o.total_price, 
            o.purchase_date, 
            o.order_status, 
            o.payment_method, 
            o.addon_total,
            od.product_id,
            od.quantity, 
            p.product_type, 
            p.unit_price,
            ad.addon_purchased
        FROM customers c 
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN order_details od ON od.order_id = o.order_id
        JOIN products p ON p.product_id = od.product_id
        LEFT JOIN addons_purchases ad ON ad.order_id = od.order_id
    """,
    bucket="report-busket",
    filename="dashboard_electronic_sales/electronic_sales.csv",
    export_format="CSV",
    gzip=False,
    on_failure_callback=notify_slack_on_failure,
)

    

    load_to_bq = GCSToBigQueryOperator(
    task_id='load_csv_to_bigquery',
    bucket='report-busket',
    source_objects=['dashboard_electronic_sales/electronic_sales.csv'],
    destination_project_dataset_table='dashboardprojectelsales.dashboard_dataset.daily_electronic_sales',
    source_format='CSV',
    skip_leading_rows=1,
    field_delimiter=',',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    autodetect=False,
    schema_fields=[
        {"name": "customer_id", "type": "INT64", "mode": "REQUIRED"},
        {"name": "age", "type": "INT64", "mode": "REQUIRED"},
        {"name": "gender", "type": "STRING", "mode": "REQUIRED"},
        {"name": "loyalty_member", "type": "BOOL", "mode": "REQUIRED"},
        {"name": "registration_date", "type": "DATE", "mode": "REQUIRED"},

        {"name": "order_id", "type": "INT64", "mode": "REQUIRED"},
        {"name": "total_price", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "purchase_date", "type": "DATETIME", "mode": "REQUIRED"},
        {"name": "order_status", "type": "STRING", "mode": "REQUIRED"},
        {"name": "payment_method", "type": "STRING", "mode": "REQUIRED"},
        {"name": "addon_total", "type": "NUMERIC", "mode": "REQUIRED"},

        {"name": "product_id", "type": "INT64", "mode": "REQUIRED"},
        {"name": "quantity", "type": "INT64", "mode": "REQUIRED"},
        {"name": "product_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "unit_price", "type": "NUMERIC", "mode": "REQUIRED"},

        {"name": "addon_purchased", "type": "STRING", "mode": "REQUIRED"}
    ],
    on_failure_callback=notify_slack_on_failure,

    
)
    dbt_run_weekly_aggregate = BashOperator(
    task_id='dbt_run_weekly_aggregate',
    bash_command=(
        f'cd {dbt_path} && '
        f'dbt run --select weekly_aggregates --profiles-dir {dbt_path} --full-refresh'
    ),
    on_failure_callback=notify_slack_on_failure,
)


    soda_raw_check >>  export_postgres_to_gcs >> load_to_bq >> dbt_run_weekly_aggregate



