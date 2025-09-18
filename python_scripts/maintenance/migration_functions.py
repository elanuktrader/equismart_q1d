import pandas as pd
import numpy as np
import psycopg2
import cx_Oracle
from config.db_config import *
from datetime import datetime, timedelta,date


            
def migrate_postgres_to_adw(pg_table, adw_table,  column_mapping, hist_date=None, columns_remove=None, primary_key_columns=None, db_action="SKIP",selected_stocks=None, custom_query=None): 
    """
    Migrate data from PostgreSQL to Oracle ADW with filtering, column removal, and duplicate handling.
    
    :param pg_table: Source PostgreSQL table name
    :param adw_table: Destination Oracle ADW table name
    :param column_mapping: Dictionary mapping PostgreSQL columns to ADW columns
    :param hist_date: Optional filter date (default: None)
    :param columns_remove: List of columns to remove before insertion (default: None)
    :param db_action: "REPLACE" or "SKIP" to control duplicate handling (default: "SKIP")
    """
    # PostgreSQL Connection
    
    pg_conn = get_processor_db_connection()
    # Create a cursor object
    pg_cursor = pg_conn.cursor()
    
    # Fetch Data from PostgreSQL
    query = custom_query
    if pg_table == "raw_data.nse_stock_cm_data":
        pg_cursor.execute(query, (selected_stocks, hist_date, hist_date, hist_date))
    else:
        pg_cursor.execute(query, (hist_date, hist_date, hist_date))
    rows = pg_cursor.fetchall()
    columns = [desc[0] for desc in pg_cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    
    # Drop specified columns if provided
    if columns_remove:
        df.drop(columns=columns_remove, inplace=True, errors='ignore')
    
    # Rename columns using mapping
    df.rename(columns=column_mapping, inplace=True)
    
    pg_cursor.close()
    pg_conn.close()
    print(f"✅ Data fetched from PostgreSQL: {df.shape[0]} rows")
    
    # Oracle ADW Connection

    adw_conn = get_data_store_adw_connection()
    adw_cursor = adw_conn.cursor()
    
    # Prepare Insert Query
    if db_action == "SKIP":
        insert_query = f"""
            INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX({adw_table}) */ INTO {adw_table} 
            ({', '.join(df.columns)}) 
            VALUES ({', '.join([':' + str(i+1) for i in range(len(df.columns))])})
        """
    elif db_action == "REPLACE":
        insert_query = f"""
            MERGE INTO {adw_table} USING DUAL 
            ON (primary_key_columns = :1) 
            WHEN MATCHED THEN 
                UPDATE SET {', '.join([f'{col} = :{i+1}' for i, col in enumerate(df.columns, start=2)])}
            WHEN NOT MATCHED THEN 
                INSERT ({', '.join(df.columns)}) 
                VALUES ({', '.join([':' + str(i+1) for i in range(len(df.columns))])})
        """
    else:
        raise ValueError("Invalid db_action. Use 'REPLACE' or 'SKIP'")
    
    # Batch insert into ADW
    def dataframe_chunk_generator(df, chunk_size=10000):
        """
        Generates chunks of rows from a Pandas DataFrame.

        Args:
            df: The Pandas DataFrame to chunk.
            chunk_size: The number of rows per chunk.

        Yields:
            A list of lists representing a chunk of rows from the DataFrame.
        """
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i:i + chunk_size].values.tolist()



    for chunk in dataframe_chunk_generator(df):
        try:
            adw_cursor.executemany(insert_query, chunk)
            adw_conn.commit()
        except cx_Oracle.DatabaseError as e:
            print(f"Error during chunk insertion: {e}")
            adw_conn.rollback() #rollback the transaction.



    # adw_cursor.executemany(insert_query, df.values.tolist())
    # adw_conn.commit()
    
    adw_cursor.close()
    adw_conn.close()
    print(f"✅ Successfully migrated {df.shape[0]} rows to Oracle ADW")



def migrate_postgres_to_adw_intraday(adw_table,  column_mapping, insert_start_date,insert_end_date,day_increment =10, columns_remove=None, primary_key_columns=None, db_action="SKIP",selected_stocks=None, custom_query=None): 
    """
    Migrate data from PostgreSQL to Oracle ADW with filtering, column removal, and duplicate handling.
    
    :param pg_table: Source PostgreSQL table name
    :param adw_table: Destination Oracle ADW table name
    :param column_mapping: Dictionary mapping PostgreSQL columns to ADW columns
    :param hist_date: Optional filter date (default: None)
    :param columns_remove: List of columns to remove before insertion (default: None)
    :param db_action: "REPLACE" or "SKIP" to control duplicate handling (default: "SKIP")
    """
    # PostgreSQL Connection
    
    pg_conn = get_processor_db_connection()
    # Create a cursor object
    pg_cursor = pg_conn.cursor()
    pg_query = custom_query

    chuck_start_date = insert_start_date

   
    

    while chuck_start_date <= insert_end_date:
        chunk_end_date = chuck_start_date + timedelta(days=day_increment - 1)
        if chunk_end_date > insert_end_date:
            chunk_end_date = insert_end_date

        print(f"Migrating data for {chuck_start_date} to {chunk_end_date}")
    
    # Fetch Data from PostgreSQL

        if selected_stocks:
                pg_cursor.execute(pg_query, (selected_stocks, chuck_start_date, chunk_end_date))
        else:
                pg_cursor.execute(pg_query, (chuck_start_date, chunk_end_date))
    
    
        rows = pg_cursor.fetchall()
        columns = [desc[0] for desc in pg_cursor.description]
        df = pd.DataFrame(rows, columns=columns)
    
        # Drop specified columns if provided
        if columns_remove:
            df.drop(columns=columns_remove, inplace=True, errors='ignore')
    
        # Rename columns using mapping
        df.rename(columns=column_mapping, inplace=True)
    

        print(f"✅ Data fetched from PostgreSQL: {df.shape[0]} rows")
    
        # Oracle ADW Connection

        adw_conn = get_data_store_adw_connection()
        adw_cursor = adw_conn.cursor()

         # Prepare ADW Insert Query
        if db_action == "SKIP":
            adw_insert_query = f"""
                INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX({adw_table}) */ INTO {adw_table} 
                ({', '.join(df.columns)}) 
                VALUES ({', '.join([':' + str(i+1) for i in range(len(df.columns))])})
            """
        elif db_action == "REPLACE":
            on_clause = ' AND '.join([f'{col} = :{i+1}' for i, col in enumerate(primary_key_columns)])
            update_clause = ', '.join([f'{col} = :{i+len(primary_key_columns)+1}' for i, col in enumerate(df.columns.difference(primary_key_columns))])
            adw_insert_query = f"""
                MERGE INTO {adw_table} USING DUAL 
                ON ({on_clause}) 
                WHEN MATCHED THEN 
                    UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN 
                    INSERT ({', '.join(df.columns)}) 
                    VALUES ({', '.join([':' + str(i+1) for i in range(len(df.columns))])})
            """
        else:
            raise ValueError("Invalid db_action. Use 'REPLACE' or 'SKIP'")
            
        
        # Batch insert into ADW
        def dataframe_chunk_generator(df, chunk_size=10000):
            """
            Generates chunks of rows from a Pandas DataFrame.

            Args:
                df: The Pandas DataFrame to chunk.
                chunk_size: The number of rows per chunk.

            Yields:
                A list of lists representing a chunk of rows from the DataFrame.
            """
            for i in range(0, len(df), chunk_size):
                yield df.iloc[i:i + chunk_size].values.tolist()



        for chunk in dataframe_chunk_generator(df):
            try:
                adw_cursor.executemany(adw_insert_query, chunk)
                adw_conn.commit()
            except cx_Oracle.DatabaseError as e:
                print(f"Error during chunk insertion: {e}")
                adw_conn.rollback() #rollback the transaction.

        print(f"✅ Successfully migrated {df.shape[0]} rows to Oracle ADW")
        chuck_start_date = chunk_end_date + timedelta(days=1)

    # adw_cursor.executemany(insert_query, df.values.tolist())
    # adw_conn.commit()
    pg_cursor.close()
    pg_conn.close()
    adw_cursor.close()
    adw_conn.close()
