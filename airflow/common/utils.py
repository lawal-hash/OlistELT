from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.decorators import dag, task


from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.decorators import task, task_group
from airflow.utils.db import provide_session
from airflow.models import XCom
from datetime import datetime, timedelta, timezone
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
import json
import pandas as pd


@task_group
def extract_data_from_postgres(table_names: list, postgres_conn_id: str = "postgres_conn_id"):
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    extract_data = {}
    for table_name in table_names:

        @task(task_id=f"fetch_data_{table_name}")
        def extract_table(table_name: str = table_names):
            results = pg_hook.run(
                handler=fetch_all_handler,
                parameters=[],
                sql=f" SELECT * FROM olist.{table_name} limit 3;",
            )
            return results

        extract_data[table_name] = extract_table(table_name=table_name)
    return extract_data


@task_group
def load_data_into_bigquery(data, bigquery_conn_id, project_id, dataset_id):
    bq_hook = BigQueryHook(gcp_conn_id=bigquery_conn_id)
    for key in data:
        record = data[key]

        @task(task_id=f"load_data_{key}")
        def load_table(key, bigquery_conn_id, project_id, dataset_id):
            record = data[key]
            schema, column_names = get_schema_from_gcs(key, bigquery_conn_id)
            bq_hook.create_empty_table(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=key,
                schema_fields=schema,
                location="europe-west3 ",
                exists_ok=True,
            )
            df = pd.DataFrame(record, columns=column_names)
            # To Do: make idempotent
            bq_hook.insert_all(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=key,
                rows=df.to_dict(orient="records"),
            )

        load_table(key, bigquery_conn_id, project_id, dataset_id)
    return "done"





@provide_session
def cleanup_xcom(session=None, **context):
    dag = context["dag"]
    dag_id = dag._dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


@provide_session
def cleanup__xcom(session=None):
    ts_limit = datetime.now(timezone.utc) - timedelta(days=2)
    session.query(XCom).filter(XCom.execution_date <= ts_limit).delete()


def get_schema_from_gcs(table_name: str, gcp_conn_id) -> list:
    url = f"gs://bigquery_schema_airflow/schema/{table_name}.json"
    gcs_bucket, gcs_object = _parse_gcs_url(url)
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    schema_fields_string = gcs_hook.download_as_byte_array(
        gcs_bucket, gcs_object
    ).decode("utf-8")
    schema_fields = json.loads(schema_fields_string)
    column_names = [item["name"] for item in schema_fields]

    return schema_fields, column_names
