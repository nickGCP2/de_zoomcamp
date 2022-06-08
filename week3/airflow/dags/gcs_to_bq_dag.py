import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

# Import Operators
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# Specify Local Variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "ny_trips_all")

INPUT_FOLDER = 'raw'
INPUT_DATASET = 'taxi'
INPUT_FILETYPE = 'parquet'
COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}

# Default Arguments to be passed to the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1
}

# Define DAG in implicit way
with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup= False,
    max_active_runs=1,
    tags=['gcs-bq']
) as dag:

    # Running tasks in a loop for different taxci datasets (Green, Yellow)
    for colour, ds_col in COLOUR_RANGE.items():

        # Operator/task to reorganize files inside GCS for easier processing
        move_files_gcs_task = GCSToGCSOperator(
            task_id=f'move_{colour}_{INPUT_DATASET}_task',
            source_bucket=BUCKET,
            source_object=f'{INPUT_FOLDER}/{colour}_{INPUT_DATASET}*.{INPUT_FILETYPE}',
            destination_bucket=BUCKET,
            destination_object=f'{colour}/{colour}_{INPUT_DATASET}',
            move_object=True
        )

        # Operator/task to create external table from GCS Bucket data lake
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f'bq_{colour}_{INPUT_DATASET}_external_table_task',
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f'{colour}_{INPUT_DATASET}_external_table'
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": f'{INPUT_FILETYPE.upper()}',
                    "sourceUris": [f"gs://{BUCKET}/{colour}/*"]
                }
            }
        )

        EXCEPT_COL = " EXCEPT (ehail_fee) " if colour=='green' else " "

        # Query to create partitioned table from external tables
        CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{INPUT_DATASET}_partitioned_table \
            PARTITION BY DATE({ds_col}) \
            AS \
            SELECT *{EXCEPT_COL}FROM {BIGQUERY_DATASET}.{colour}_{INPUT_DATASET}_external_table;"
        )

        # Operator/task to run query of creating partitioned table
        bq_create_partitioned_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_{colour}_{INPUT_DATASET}_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False
                }
            }
        )

        # move_files_gcs_task
        move_files_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job
        
