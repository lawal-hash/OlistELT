import datetime

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

DAG_ID = "integration"
with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    with TaskGroup("extract_from_postgres", tooltip="Tasks for extracting data from postgres") as postgres:

        extract_query = [
            SQLExecuteQueryOperator(
                conn_id="postgres_conn_id",
                task_id=f"extract_{table_name}",
                parameters=[100],
                sql=f" SELECT * FROM olist.{table_name} LIMIT 3;",
                split_statements=True,
                return_last=False,
            )
            for table_name in [
                "geolocation",
                "customers",
                "sellers",
                "orders",
                "product_category_name_translation",
                "products",
                "order_payments",
                "order_reviews",
                "order_items",
            ]
        ]


    with TaskGroup("load_to_bigquery", tooltip="Tasks for loading data into bigquery") as data_warehouse:
        load = EmptyOperator(task_id="start")

    extract_query >> load
    postgres >> data_warehouse
