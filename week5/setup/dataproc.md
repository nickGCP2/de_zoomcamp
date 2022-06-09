Commands used for running spark job via terminal

URL="spark://ny-trips-vm.europe-west1-b.c.ny-trips-de.internal:7077"

spark-submit \
  --master="${URL}" \
  spark_local.py \
    --input_green=data/pq/green/2021/*/ \
    --input_yellow=data/pq/yellow/2021/*/ \
    --output=data/report-2021

-----

Arguments provided to dataproc cluster job UI
--input_green=gs://ny-trips-de_ny_taxi_data_lake/pq/green/2021/*/
--input_yellow=gs://ny-trips-de_ny_taxi_data_lake/pq/yellow/2021/*/
--output=gs://ny-trips-de_ny_taxi_data_lake/report-2021

-----
"clusterName": "de-zoomcamp-spark-cluster"
"mainPythonFileUri": "gs://ny-trips-de_ny_taxi_data_lake/code/spark_local.py",
"args": [
"--input_green=gs://ny-trips-de_ny_taxi_data_lake/pq/green/2021/*/",
"--input_yellow=gs://ny-trips-de_ny_taxi_data_lake/pq/yellow/2021/*/",
"--output=gs://ny-trips-de_ny_taxi_data_lake/report-2021"

----

gcloud command

gcloud dataproc jobs submit pyspark \
  --cluster=de-zoomcamp-spark-cluster \
  --region=us-central1 \
  gs://ny-trips-de_ny_taxi_data_lake/code/spark_local.py \
  -- \
    --input_green=gs://ny-trips-de_ny_taxi_data_lake/pq/green/2020/*/ \
    --input_yellow=gs://ny-trips-de_ny_taxi_data_lake/pq/yellow/2020/*/ \
    --output=gs://ny-trips-de_ny_taxi_data_lake/report-2020