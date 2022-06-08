import os

from time import time 

import pandas as pd
from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, csv_file):
    print(table_name, csv_file)

    # Connecting to postgresql
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    print("Connection established")

    ### Reading data
    # reading first 100,000 rows of downloaded data
    t_start = time()
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)
    df = next(df_iter)

    # Changing the data type to datetime for the dates
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Creating table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Adding first iteration to the table
    df.to_sql(name=table_name, con=engine, if_exists='append')
    t_end = time()
    
    print('inserted first chunk in %.3f second' % (t_end-t_start))

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