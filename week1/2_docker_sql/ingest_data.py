#!/usr/bin/env python
# coding: utf-8

## Getting data in iterations from the local file

### Importing required packages
import pandas as pd
from sqlalchemy import create_engine
import argparse
from time import time 
import os

def main(params):

    user = params.user
    password = params.password
    port = params.port
    host = params.host
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    # Downloading data
    os.system(f"wget {url} -O {csv_name}")

    ### Reading data
    # reading first 100,000 rows of downloaded data
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    # Changing the data type to datetime for the dates
    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Connecting to postgresql
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print("Connection established")

    # Creating table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Adding first iteration to the table
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print("First 100000 rows added")

    ### Adding remaining data in iterations
    while True:
        t_start = time()
        
        # Getting data
        df = next(df_iter)
        
        # Changing the data type to datetime for the dates
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print('inserted another chunk in %.3f second' % (t_end-t_start))

    # Checking if all the records are added to the table
    # query = """
    # SELECT 
    #     COUNT(1)
    # FROM yellow_taxi_data;
    # """

    # pd.read_sql(query, con=engine)

if __name__ == '__main__':
    # Parsing command line argumemts
    parser = argparse.ArgumentParser(description='Ingest CSV Data to Postgres')

    # user, password, host, port, database name, table name
    # url of the CSV File
    
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table for postgres')
    parser.add_argument('--url', help='URL for getting the data')

    args = parser.parse_args()
    
    main(args)  
    
    
# print(args.accumulate(args.integers))

# # URl to extract data from
# url = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv'
# # we will use wget and download this data on the local file
