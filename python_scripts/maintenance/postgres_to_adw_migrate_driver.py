import pandas as pd
from python_scripts.production.etl_functions import read_default_stock_list
from python_scripts.maintenance.migration_functions import *
from datetime import datetime, timedelta,date
# File paths
import yaml
yaml_file = "./config/maintenance_config/postgres_to_adw.yaml"
Stock_primary_file_path = './config/production_config/NSE_Stocks_11.csv'  # Update with your CSV file path
selected_symbols = read_default_stock_list(Stock_primary_file_path)

# --- User Inputs ---
Insertion_date = date(2025,3,1)  #  input None for all the dates
Insertion_date = None
db_action = "SKIP"  # "REPLACE" or "SKIP"
selected_tables = "raw_data.eod_participant_vol" # input None for all the tables
#selected_tables = None # input None for all the tables

Intraday_data_insert_start_date = date(2025,1,1)
Intraday_data_insert_end_date = date(2025,2,28)
Increment_chunks = 10 # in days  


def read_yaml_config(file_path):
    """Reads YAML file and returns table configurations."""
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config['tables']

def migrate_tables(yaml_file, hist_date=None, selected_stocks=None, db_action="SKIP", selected_tables=None):
    """Migrates all or selected tables from PostgreSQL to ADW."""
    tables = read_yaml_config(yaml_file)

    # Filter tables if specific ones are selected
    if selected_tables:
        tables = [table for table in tables if table['pg_table'] in selected_tables]

    for table in tables:
        if table['pg_table'] == "raw_data.nse_stock_cm_data":
            selected_stocks = selected_symbols
            stock_array = "{" + selected_stocks + "}"
        else:
            selected_stocks = None

        if (table['pg_table'] == "raw_data.nse_stock_cm_data") or (table['pg_table'] == "raw_data.nse_stock_fno_data"):

            print(f"Migrating table: {table['pg_table']} to {table['adw_table']}")
            print(f"Column mapping: {table['column_mapping']}")
            print(f"Historical date: {hist_date}")
            print(f"Columns to remove: {table.get('columns_remove') or None}")
            print(f"Primary key columns: {table.get('primary_key_columns')}")
            print(f"Database action: {db_action}")
            print(f"Selected stocks: {selected_stocks}")
            print(f"Custom query: {table.get('custom_query') or None}")
            print('=================================================================================================')

            migrate_postgres_to_adw_intraday(
                adw_table=table['adw_table'],
                column_mapping=table['column_mapping'],
                insert_start_date = Intraday_data_insert_start_date,
                insert_end_date = Intraday_data_insert_end_date,
                day_increment= Increment_chunks,
                columns_remove=table.get('columns_remove') or None,
                primary_key_columns=table.get('primary_key_columns') or None,
                db_action=db_action,  # Passed as function parameter
                selected_stocks=stock_array,  # Passed as function parameter
                custom_query=table.get('custom_query') or None
            )

            
        else:
            hist_date = Insertion_date
        
            print(f"Migrating table: {table['pg_table']} to {table['adw_table']}")
            print(f"Column mapping: {table['column_mapping']}")
            print(f"Historical date: {hist_date}")
            print(f"Columns to remove: {table.get('columns_remove') or None}")
            print(f"Primary key columns: {table.get('primary_key_columns')}")
            print(f"Database action: {db_action}")
            print(f"Selected stocks: {selected_stocks}")
            print(f"Custom query: {table.get('custom_query') or None}")
            print('=================================================================================================')

            migrate_postgres_to_adw(
                pg_table=table['pg_table'],
                adw_table=table['adw_table'],
                column_mapping=table['column_mapping'],
                hist_date=hist_date,  # Passed as function parameter
                columns_remove=table.get('columns_remove') or None,
                primary_key_columns=table.get('primary_key_columns') or None,
                db_action=db_action,  # Passed as function parameter
                selected_stocks=selected_stocks,  # Passed as function parameter
                custom_query=table.get('custom_query') or None
            )

# --- User Inputs ---


migrate_tables(yaml_file, hist_date=Insertion_date, selected_stocks=selected_symbols, db_action=db_action, selected_tables=selected_tables)




    










