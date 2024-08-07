import sys

sys.path.append("common")
from datetime import datetime, timedelta, timezone
from airflow.decorators import task_group
import pandas as pd
from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from utils import extract_table, load_table, read_table,cleanup_xcom
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

DAG_ID = "integration"
project_id = "olistelt"
dataset_id = "olist_data"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    # on_success_callback=cleanup__xcom
) as dag:

    @task_group
    def extract_load(table_name):
        data = extract_table(table_name=table_name, postgres_conn_id="postgres_conn_id")
        load_table(data, "gcp_conn_id", project_id, dataset_id, table_name)
        
    xcom_cleaner = PythonOperator(
    task_id='delete-old-xcoms',
    python_callable=cleanup_xcom)
    
    email  = EmailOperator(
        task_id="send_email",
        to="lawalsophia4@gmail.com",
        subject="Data Pipeline Execution",
        html_content="Data Pipeline has been executed successfully",
    )

    extract_load.expand(table_name=read_table()) >> xcom_cleaner 
    xcom_cleaner >> email



    # extract_tasks >> load_data("customers", "gcp_conn_id")
