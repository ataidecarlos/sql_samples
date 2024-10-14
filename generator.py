import os, time

import struct
from azure.identity import ManagedIdentityCredential

import pandas as pd
import numpy as np
import sqlalchemy as sa


def generate_random_arrays(num_rows: int, batch_id:int =None):
    
    # partition key
    if batch_id is None:
        int_batch_id = np.random.randint(1, 1000, num_rows)
    else:
        int_batch_id = np.full(num_rows, batch_id)
    
    # int
    int_array_1 = np.random.randint(-1_000_000, 1_000_000, num_rows)
    int_array_2 = np.random.randint(-1_000_000, 1_000_000, num_rows)
    int_array_3 = np.random.randint(-1_000_000, 1_000_000, num_rows)
    
    # float
    float_array_1 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_2 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_3 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_4 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_5 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_6 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_7 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_8 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_9 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_10 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_11 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_12 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_13 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_14 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_15 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_16 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_17 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_18 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_19 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_20 = np.random.uniform(-1_000_000, 1_000_000, num_rows)
    float_array_21 = np.random.uniform(-1_000_000, 1_000_000, num_rows)

    return (
        int_batch_id,
        int_array_1, int_array_2, int_array_3,
        float_array_1, float_array_2, float_array_3, float_array_4, float_array_5,
        float_array_6, float_array_7, float_array_8, float_array_9, float_array_10,
        float_array_11, float_array_12, float_array_13, float_array_14, float_array_15,
        float_array_16, float_array_17, float_array_18, float_array_19, float_array_20,
        float_array_21
    )


def create_dataframe(arrays):
    column_names = [
        'batch_id', 
        'col_int_1', 'col_int_2', 'col_int_3', 
        'col_float_1', 'col_float_2', 'col_float_3', 'col_float_4', 'col_float_5',
        'col_float_6', 'col_float_7', 'col_float_8', 'col_float_9', 'col_float_10',
        'col_float_11', 'col_float_12', 'col_float_13', 'col_float_14', 'col_float_15',
        'col_float_16', 'col_float_17', 'col_float_18', 'col_float_19', 'col_float_20',
        'col_float_21'
    ]
    return pd.DataFrame(dict(zip(column_names, arrays)))


def get_access_token():
    credential = ManagedIdentityCredential() # system assigned managed identity
    token = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    access_token_struct = struct.pack("=i", len(token)) + token

    return access_token_struct


def get_connection_string():
    SERVER = "ataide-sql-ne.database.windows.net"
    DATABASE = "partitions-test"
    CONNECTION_STRING = f"mssql+pyodbc:///?odbc_connect=DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE}"

    return CONNECTION_STRING


def get_connection():
    connection_string = get_connection_string()
    access_token = get_access_token()

    engine = sa.create_engine(connection_string,connect_args={"attrs_before": {1256: access_token}})
    
    try:
        return engine
    except Exception as error:
        print(f'Error | {error.orig.args[0][0]}')
        return -1     


def test_sqldb():
    connection_engine = get_connection()
    if connection_engine == -1: return

    result_df = pd.read_sql_query('SELECT @@SERVERNAME AS Server, DB_NAME() AS DatabaseName, USER_NAME() AS UserName, SUSER_SNAME() AS AppId',connection_engine)
    print(result_df)


def insert_dataframe_to_sql(df, table_name):
    connection_engine = get_connection()
    if connection_engine == -1: return

    try:
        df.to_sql(table_name, con=connection_engine, if_exists='append', index=False)
        #print(f"Data inserted into {table_name} successfully.")
    except Exception as error:
        print(f'Error inserting data: {error}')

def insert_dataframe_to_csv(df, file_name):

    try:
        df.to_csv(file_name, index=False)
        #print(f"Data inserted into {file_name} successfully.")
    except Exception as error:
        print(f'Error inserting data: {error}')


if __name__ == "__main__":
    os.system("clear")

    # Test SQL connection
    # test_sqldb()
    

    for i in range(2020,2026):
        # Generate random arrays and create DataFrame
        num_rows = 1_000_000
        
        generator_time = time.time()
        arrays = generate_random_arrays(num_rows,i)
        generator_time = time.time() - generator_time
        
        dataframe_time = time.time()
        df = create_dataframe(arrays)
        dataframe_time = time.time() - dataframe_time

        # Insert data into SQL Database or CSV file
        csv_time = time.time()
        insert_dataframe_to_csv(df, f"user_data_{i}.csv")
        csv_time = time.time() - csv_time

        sql_time = time.time()
        insert_dataframe_to_sql(df, "user_data")
        sql_time = time.time() - sql_time

        print(f"Batch ID: {i}")
        # print(f"Time taken to generate random arrays: {generator_time:.2f} seconds")
        # print(f"Time taken to create DataFrame: {dataframe_time:.2f} seconds")
        # print(f"Time taken to insert DataFrame to CSV: {csv_time:.2f} seconds")
        print(f"Time taken to insert DataFrame to SQL: {sql_time:.2f} seconds")