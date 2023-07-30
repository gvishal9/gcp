import datetime
from airflow import models
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.utils.dates import days_ago
from airflow.operators import python_operator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.models.param import Param

default_args = {
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
    "email": "gogineni.vishal@gmail.com",
    "email_on_failure": True,
    "email_on_retry": True,
}
GCS_SOURCE = Variable.get("GCS_SOURCE")
GCS_DEST = Variable.get("GCS_DEST")
PROJECT = Variable.get("PROJECT")
DATASET_NAME = Variable.get("DATASET_NAME")
TABLE = Variable.get("TABLE")

with models.DAG(
    "gcs_to_bq", schedule_interval="0 7 * * *", default_args=default_args
) as dag:

    load_gcs_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="load_gcs_bq",
        bucket=f"{GCS_SOURCE}",
        source_objects=['raw/*.csv'],
        destination_project_dataset_table=f"{PROJECT}.{DATASET_NAME}.{TABLE}",
        source_format="csv",
        skip_leading_rows=1,
        field_delimiter=",",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        schema_fields=[
            {"name": "country_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country_value", "type": "STRING", "mode": "NULLABLE"},
            {"name": "date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "indicator_value", "type": "STRING", "mode": "NULLABLE"},
            {"name": "lastupdated", "type": "STRING", "mode": "NULLABLE"},
            {"name": "page", "type": "STRING", "mode": "NULLABLE"},
            {"name": "value", "type": "STRING", "mode": "NULLABLE"},
        ],
        dag=dag
    )
move_files = GCSToGCSOperator(
    task_id="move_files",
    source_bucket=f"{GCS_SOURCE}",
    source_object="raw/*.csv",
    destination_bucket=f"{GCS_DEST}",
    move_object=True,
    dag=dag
)

start = DummyOperator(task_id="start", retries=3, dag=dag)
end = DummyOperator(task_id="end", retries=3, dag=dag)

start >> load_gcs_bq >> move_files >> end
