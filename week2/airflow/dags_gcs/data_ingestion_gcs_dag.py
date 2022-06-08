import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

# Import Operators
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Import other libraries required for converting to parquet and uploading to GCP
import pyarrow.csv as pv
import pyarrow.parquet as pq
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# Specify Local Variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "ny_trips_all")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
dataset_file = "yellow_tripdata_2021-01.csv"
parquet_file = dataset_file.replace('.csv', '.parquet')
dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"

# Defining python functions to be called inside DAG
# Function to convert CSV to Parquet
def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

# Function to upload Parquet File to GCS Bucket
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    # Workaround to prevent timeout for files > 6MB on 800kbps upload speed
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024

    # uploading file
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

# Default Arguments to be passed to the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['de-zm-localpq-gcs-bq']
) as dag:

    # Task/Operator for downloading Dataset
    download_dataset_task = download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sS {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    # Task for converting CSV to Parquet
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs = {
            "src_file": f"{path_to_local_home}/{dataset_file}"
        }        
    )

    # Task for uploading converted parquet file to GCS
    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket":BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}"
        }
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    # Task for uploading file from GCS Bucket to BigQuery
    bigquery_extrernal_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_extrernal_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table"                
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"]
            }
        }
    )

    download_dataset_task >> format_to_parquet_task >> upload_to_gcs_task >> bigquery_extrernal_table_task



