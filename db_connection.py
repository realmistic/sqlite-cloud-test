import os
from dotenv import load_dotenv
import pandas as pd
import sqlitecloud
import numpy as np

# Load environment variables from .env file
load_dotenv()

def get_db_connection():
    """
    Get a connection to the SQLite Cloud database.
    
    Returns:
        SQLite Cloud connection object
    """
    if "SQLITECLOUD_URL" not in os.environ:
        raise ValueError("SQLITECLOUD_URL environment variable is not set")
    
    url = os.environ["SQLITECLOUD_URL"]
    try:
        # Connect to SQLite Cloud
        conn = sqlitecloud.connect(url)
        print("SQLite Cloud connection successful")
        return conn
    except Exception as e:
        print(f"Error connecting to SQLite Cloud: {str(e)}")
        raise

def read_data(query):
    """
    Read data from the database using a SQL query.
    
    Args:
        query: SQL query to execute
        
    Returns:
        pandas DataFrame with the query results
    """
    conn = get_db_connection()
    try:
        # Execute query and convert to pandas DataFrame
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Get column names from cursor description
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            return df
        else:
            # Query didn't return any data (e.g., INSERT, UPDATE, DELETE)
            return pd.DataFrame()
    finally:
        conn.close()

def write_data(df, table_name, if_exists='replace'):
    """
    Write a pandas DataFrame to the database.
    
    Args:
        df: pandas DataFrame to write
        table_name: Name of the table to write to
        if_exists: What to do if the table exists ('replace', 'append', 'fail')
    """
    if df.empty:
        print("DataFrame is empty, nothing to write")
        return
    
    # Create a copy of the DataFrame to avoid modifying the original
    df_copy = df.copy()
    
    # Convert pandas Timestamp objects to ISO-formatted strings
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].astype(str)
        
        # Convert numpy int64/float64 to Python int/float
        elif pd.api.types.is_integer_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].astype(int)
        elif pd.api.types.is_float_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].astype(float)
    
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_exists = cursor.fetchone() is not None
        
        if table_exists and if_exists == 'replace':
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            table_exists = False
        elif table_exists and if_exists == 'fail':
            raise ValueError(f"Table '{table_name}' already exists")
        
        # Create table if it doesn't exist
        if not table_exists:
            # Generate column definitions based on DataFrame dtypes
            columns = []
            for col in df_copy.columns:
                dtype = df_copy[col].dtype
                if pd.api.types.is_numeric_dtype(dtype):
                    if pd.api.types.is_integer_dtype(dtype):
                        sql_type = "INTEGER"
                    else:
                        sql_type = "REAL"
                else:
                    sql_type = "TEXT"
                columns.append(f"{col} {sql_type}")
            
            create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
            cursor.execute(create_table_sql)
        
        # Insert data
        placeholders = ", ".join(["?"] * len(df_copy.columns))
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples for insertion
        data = [tuple(row) for row in df_copy.values]
        
        # Insert in batches to avoid potential issues with large datasets
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            cursor.executemany(insert_sql, batch)
        
        conn.commit()
        print(f"Successfully wrote {len(df_copy)} rows to table {table_name}")
    finally:
        conn.close()
