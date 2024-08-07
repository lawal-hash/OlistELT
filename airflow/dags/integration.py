import sys

sys.path.append("common")
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.task_group import TaskGroup
from utils import extract_data_from_postgres, get_schema_from_gcs, load_data_into_bigquery

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
    # Task 1
    data = extract_data_from_postgres(
        table_names=["customers", "orders"],
        postgres_conn_id="postgres_conn_id",
    )
    output=load_data_into_bigquery(data, "gcp_conn_id", project_id, dataset_id)
    
    
    data >> output
    
    
    
    
    
    
    
    
    
    
    
    
    
    

    # xcom_cleaner = PythonOperator(
    #    task_id='delete-old-xcoms',
    #    python_callable=cleanup_xcom)

    # extract_tasks >> load_data("customers", "gcp_conn_id")

