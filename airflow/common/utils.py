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


@task
def read_table():
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
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    results = pg_hook.run(
        handler=fetch_all_handler,
        parameters=[],
        sql=f" SELECT * FROM olist.{table_name};",
    )
    return results


@task
def load_table(
    data, bigquery_conn_id, project_id, dataset_id, table_name, chunk_size=5000
):
    bq_hook = BigQueryHook(gcp_conn_id=bigquery_conn_id)
    schema, column_names = get_schema_from_gcs(table_name, bigquery_conn_id)
    bq_hook.create_empty_table(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_name,
        schema_fields=schema,
        location="europe-west3 ",
        exists_ok=True,
    )
    # ToDo: Implement idempotent insert
    df = pd.DataFrame(data, columns=column_names)
    length = len(df)
    for i in range(0, length, chunk_size):
        rows = df.iloc[i:i + chunk_size, :].to_json(
            orient="records", date_format="iso"
        )

        bq_hook.insert_all(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_name,
            rows=loads(rows),
        )


@provide_session
def cleanup_xcom(session=None, **context):
    dag = context["dag"]
    dag_id = dag._dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


@provide_session
def cleanup__xcom(session=None):
    ts_limit = datetime.now(timezone.utc) - timedelta(days=2)
    session.query(XCom).filter(XCom.execution_date <= ts_limit).delete()


def get_schema_from_gcs(table_name: str, gcp_conn_id) -> tuple:
    url = f"gs://bigquery_schema_airflow/schema/{table_name}.json"
    gcs_bucket, gcs_object = _parse_gcs_url(url)
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    schema_fields_string = gcs_hook.download_as_byte_array(
        gcs_bucket, gcs_object
    ).decode("utf-8")
    schema_fields = json.loads(schema_fields_string)
    column_names = [item["name"] for item in schema_fields]

    return schema_fields, column_names
