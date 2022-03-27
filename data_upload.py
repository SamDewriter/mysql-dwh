from matplotlib.pyplot import table
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os
import pymysql

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name=params.table_name
    
    # Download the csv file
    url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"
    csv_name = "output.csv"
    os.system(f"wget {url} -O {csv_name}")
    
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    
    pd.io.sql.get_schema(df, name=table_name, con=engine)
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    df.head(n=0).to_sql(table_name, con=engine, if_exists="replace")
    
    try:
        while True:
            t_start = time()
            df = next(df_iter)
            
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            
            df.to_sql("yellow_taxi_data", con=engine, if_exists="append")

            t_end = time()
            print("inserted another chunk..., took %.3f" % (t_end - t_start))

    except:
        print("The maximum iteration reached")
        print("Data has finished loading into the database")
        
        
# User
# Password
# host
# port
# database name
# table name

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingesting CSV data into MySQL database')
    parser.add_argument('--user', help='username for MySQL')
    parser.add_argument('--password', help='password for MySQL')
    parser.add_argument('--host', help='hostname for MySQL')
    parser.add_argument('--port', help='port for running MySQL')
    parser.add_argument('--table_name', help='table name of MySQL')
    parser.add_argument('--db', help='db name of postgres')
   
    args = parser.parse_args()
    
    main(args)