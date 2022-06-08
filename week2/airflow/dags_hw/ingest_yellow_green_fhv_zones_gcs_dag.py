from datetime import datetime
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

# Specify Local Variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# Defining python functions to be called inside DAG
# Function to convert CSV to Parquet
def format_to_parquet(src_file, dest_file):
    print("Entered Parquet format DAG")
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)
    print("File converted to Parquet")

# Function to upload Parquet File to GCS Bucket
def upload_to_gcs(bucket, object_name, local_file):
    print("Entered Upload to GCS DAG")

    # uploading file
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    print("Uploaded File to GCS")

# Default Arguments to be passed to the DAG
default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1
}

# Define common DAG Function
def download_parquetize_upload_dag(
    dag,
    url_template,
    csv_file,
    parquet_file,
    gcs_bucket_path
):
    with dag:

        # Task/Operator for downloading Dataset
        download_dataset_task = download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {csv_file}"
        )

        # Task for converting CSV to Parquet
        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs = {
                "src_file": csv_file,
                "dest_file" : parquet_file
            }        
        )

        # Task for uploading converted parquet file to GCS
        upload_to_gcs_task = PythonOperator(
            task_id="upload_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket":BUCKET,
                "object_name": gcs_bucket_path,
                "local_file": parquet_file
            }        
        )

        rm_csv_parquet_task = BashOperator(
            task_id="rm_csv_parquet_task",
            bash_command=f'rm {csv_file} {parquet_file}',        
        )

        download_dataset_task >> format_to_parquet_task >> upload_to_gcs_task >> rm_csv_parquet_task

URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data"

# Downloading Yellow Taxi Data
YELLOW_URL_TEMPLATE = URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv" 
YELLOW_FILE_OUTPUT = AIRFLOW_HOME + "/yellow_taxi_{{ execution_date.strftime('%Y-%m') }}.csv"
YELLOW_PARQUET_FILE = YELLOW_FILE_OUTPUT.replace('.csv', '.parquet')
YELLOW_GCS_PATH = "raw/yellow_taxi/{{ execution_date.strftime('%Y') }}/yellow_taxi_{{ execution_date.strftime('%Y-%m') }}.parquet"

yellow_taxi_data_dag = DAG(
    dag_id="YellowDataIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    default_args=default_args,
    catchup= True,
    max_active_runs=1
)

download_parquetize_upload_dag(
    dag=yellow_taxi_data_dag,
    url_template=YELLOW_URL_TEMPLATE,
    csv_file=YELLOW_FILE_OUTPUT,
    parquet_file=YELLOW_PARQUET_FILE,
    gcs_bucket_path=YELLOW_GCS_PATH
)

# Downloading Green Taxi Data
GREEN_URL_TEMPLATE = URL_PREFIX + "/green_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv" 
GREEN_FILE_OUTPUT = AIRFLOW_HOME + "/green_taxi_{{ execution_date.strftime('%Y-%m') }}.csv"
GREEN_PARQUET_FILE = GREEN_FILE_OUTPUT.replace('.csv', '.parquet')
GREEN_GCS_PATH = "raw/green_taxi/{{ execution_date.strftime('%Y') }}/green_taxi_{{ execution_date.strftime('%Y-%m') }}.parquet"

green_taxi_data_dag = DAG(
    dag_id="GreenDataIngestionDag",
    schedule_interval="0 7 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    default_args=default_args,
    catchup= True,
    max_active_runs=3
)

download_parquetize_upload_dag(
    dag=green_taxi_data_dag,
    url_template=GREEN_URL_TEMPLATE,
    csv_file=GREEN_FILE_OUTPUT,
    parquet_file=GREEN_PARQUET_FILE,
    gcs_bucket_path=GREEN_GCS_PATH
)

# https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2019-01.csv
# Downloading FHV Taxi Data
FHV_URL_TEMPLATE = URL_PREFIX + "/fhv_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv" 
FHV_FILE_OUTPUT = AIRFLOW_HOME + "/fhv_{{ execution_date.strftime('%Y-%m') }}.csv"
FHV_PARQUET_FILE = FHV_FILE_OUTPUT.replace('.csv', '.parquet')
FHV_GCS_PATH = "raw/fhv_taxi/{{ execution_date.strftime('%Y') }}/fhv_taxi_{{ execution_date.strftime('%Y-%m') }}.parquet"

fhv_taxi_data_dag = DAG(
    dag_id="FHVDataIngestionDag",
    schedule_interval="0 8 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup= True,
    max_active_runs=3
)

download_parquetize_upload_dag(
    dag=fhv_taxi_data_dag,
    url_template=FHV_URL_TEMPLATE,
    csv_file=FHV_FILE_OUTPUT,
    parquet_file=FHV_PARQUET_FILE,
    gcs_bucket_path=FHV_GCS_PATH
)

# Downloading Zone Data
ZONE_URL_TEMPLATE = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv" 
ZONE_FILE_OUTPUT = AIRFLOW_HOME + "/zone_lookup_data.csv"
ZONE_PARQUET_FILE = ZONE_FILE_OUTPUT.replace('.csv', '.parquet')
ZONE_GCS_PATH = "raw/zone_lookup/zone_lookup.parquet"

zone_lookup_data_dag = DAG(
    dag_id="ZoneDataIngestionDag",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup= True,
    max_active_runs=3
)

download_parquetize_upload_dag(
    dag=zone_lookup_data_dag,
    url_template=ZONE_URL_TEMPLATE,
    csv_file=ZONE_FILE_OUTPUT,
    parquet_file=ZONE_PARQUET_FILE,
    gcs_bucket_path=ZONE_GCS_PATH
)
