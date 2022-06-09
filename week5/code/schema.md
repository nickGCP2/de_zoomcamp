types.StructType([
  StructField('hvfhs_license_num', StringType(), True),
  StructField('dispatching_base_num', StringType(), True),
  StructField('originating_base_num', StringType(), True),
  StructField('request_datetime', TimestampType(), True),
  StructField('on_scene_datetime', TimestampType(), True),
  StructField('pickup_datetime', TimestampType(), True),
  StructField('dropoff_datetime', TimestampType(), True),
  StructField('PULocationID', IntegerType(), True),
  StructField('DOLocationID', IntegerType(), True),
  StructField('trip_miles', DoubleType(), True),
  StructField('trip_time', LongType(), True),
  StructField('base_passenger_fare', DoubleType(), True),
  StructField('tolls', DoubleType(), True),
  StructField('bcf', DoubleType(), True),
  StructField('sales_tax', DoubleType(), True),
  StructField('congestion_surcharge', DoubleType(), True),
  StructField('airport_fee', DoubleType(), True),
  StructField('tips', DoubleType(), True),
  StructField('driver_pay', DoubleType(), True),
  StructField('shared_request_flag', StringType(), True),
  StructField('shared_match_flag', StringType(), True),
  StructField('access_a_ride_flag', StringType(), True),
  StructField('wav_request_flag', StringType(), True),
  StructField('wav_match_flag', StringType(), True)
])

------------
types.StructType([
  types.StructField('hour', types.TimestampType(), True),
  types.StructField('zone', types.IntegerType(), True),
  types.StructField('revenue', types.DoubleType(), True),
  types.StructField('count', types.IntegerType(), True)
])

