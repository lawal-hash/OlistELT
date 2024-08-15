import json
from datetime import datetime, timedelta, timezone
from json import loads

import pandas as pd
from airflow.decorators import task
from airflow.models import XCom
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.db import provide_session
from google.cloud.bigquery import LoadJobConfig
@task
def read_table():
    """ Return list of tables names to extract """
    return [
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


@task
def extract_table(table_name: str, postgres_conn_id: str):
    """ Extract data from Postgres table """
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    results = pg_hook.run(
        handler=fetch_all_handler,
        parameters=[],
        sql=f" SELECT * FROM olist.{table_name};",
    )
    return results


@task
def load_table(
    data, bigquery_conn_id, project_id, dataset_id, table_name, chunk_size=15000
):
    """ Load data into BigQuery table """
    bq_hook = BigQueryHook(gcp_conn_id=bigquery_conn_id)
    if not bq_hook.table_exists(dataset_id=dataset_id, table_id=table_name):
        # Create table if not exists
        bq_hook.create_empty_table(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    table_id=table_name,
                    location="europe-west3",
                    exists_ok=True,
                )
    schema, column_names = get_schema_from_gcs(table_name, bigquery_conn_id)

    job_config = LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )
    client = bq_hook.get_client()
    df = pd.DataFrame(data, columns=column_names)
    #length = len(df)
    #for i in range(0, length, chunk_size):
    #    rows = df.iloc[i:i + chunk_size, :]
    client.load_table_from_dataframe(df, f"{dataset_id}.{table_name}", location="europe-west3",job_config=job_config)


@provide_session
def cleanup_xcom(session=None, **context):
    """ Cleanup XCom data for the current DAG """
    dag = context["dag"]
    dag_id = dag._dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


def get_schema_from_gcs(table_name: str, gcp_conn_id) -> tuple:
    """ Get schema from GCS """
    url = f"gs://bigquery_schema_airflow/schema/{table_name}.json"
    gcs_bucket, gcs_object = _parse_gcs_url(url)
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    schema_fields_string = gcs_hook.download_as_byte_array(
        gcs_bucket, gcs_object
    ).decode("utf-8")
    schema_fields = json.loads(schema_fields_string)
    column_names = [item["name"] for item in schema_fields]

    return schema_fields, column_names
