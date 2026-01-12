import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # Check if file already exists, else download
    if os.path.exists(url):
        csv_name = url
        print(f"File {csv_name} found locally.")
    else:
        if url.endswith('.csv.gz'):
            csv_name = 'output.csv.gz'
        else:
            csv_name = 'output.csv'

        # Download the file
        print(f'Downloading {url} to {csv_name}...')
        os.system(f"curl -L {url} -o {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Handle parquet files separately
    if url.endswith('.parquet'):
        print("Reading parquet file...")
        df = pd.read_parquet(csv_name)
        
        # Create the table schema (dropping if exists)
        df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
        
        # Insert data in chunks (simulated for parquet as it loads all in memory usually, 
        # but for this size it's fine to just load all or chunk if we convert to iterator)
        # For simplicity with parquet, we insert all at once or in chunks if large.
        # This dataset is likely small enough for one go or we can chunk the dataframe.
        
        chunk_size = 100000
        total_rows = len(df)
        print(f"Inserting {total_rows} rows...")
        
        t_start = time()
        df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=chunk_size)
        t_end = time()
        
        print(f"Inserted all rows in {t_end - t_start:.3f} seconds")

    else:
        # Assume CSV/CSV.gz
        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

        df = next(df_iter)

        # Convert datetime columns if they exist (adjust for specific dataset)
        if 'lpep_pickup_datetime' in df.columns:
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        if 'lpep_dropoff_datetime' in df.columns:
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

        df.to_sql(name=table_name, con=engine, if_exists='append')

        try:
            while True:
                t_start = time()
                df = next(df_iter)

                if 'lpep_pickup_datetime' in df.columns:
                    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                if 'lpep_dropoff_datetime' in df.columns:
                    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

                df.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()

                print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print("Finished ingesting data.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV/Parquet data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)
