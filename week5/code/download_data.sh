set -e

TAXI_TYPE=$1
YEAR=$2
URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"

# https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2020-01.parquet

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`

  URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

  LOCAL_FOLDER="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
  LOCAL_PATH="${LOCAL_FOLDER}/${LOCAL_FILE}"

  echo "Making folder ${LOCAL_FOLDER}"
  mkdir -p ${LOCAL_FOLDER}

  echo "Downloading file ${URL} as ${LOCAL_PATH}"
  wget ${URL} -O ${LOCAL_PATH}

done