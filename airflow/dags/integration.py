import datetime

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import  task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.hooks.sql import  fetch_all_handler
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

DAG_ID = "integration"
with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    with TaskGroup("extract") as extract:
        extract_tasks = []
        for  table_name in ["customers", "sellers", "orders"]:
            @task(multiple_outputs=True, task_id=f"fetch_data_{table_name}")
            def fetch_data(table_name: str = "customers", postgres_conn_id: str = "postgres_conn_id",):
                pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
                results = pg_hook.run(
                    handler = fetch_all_handler,
                    parameters = [],
                    sql=f" SELECT * FROM olist.{table_name} limit 3;",
                )    
                return {table_name: results}
            extract_tasks.append(fetch_data(table_name=table_name))
    
    
    
    
    def load_data(extract_tasks):
        bq_hook = BigQueryHook(gcp_conn_id="gcp_conn_id")
        bq_hook.create_empty_table()
        client = bq_hook.get_client()
        
    
    
    
    #fetch_data(table_name="customers", postgres_conn_id= "postgres_conn_id")



