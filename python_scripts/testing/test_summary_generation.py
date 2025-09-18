import psycopg2
from psycopg2.extras import execute_values
import sys
import os
import pandas as pd
sys.path.append('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/config')
sys.path.append('c:/Users/elan4/OneDrive/Documents/GitHub/equismart')
sys.path.append('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/sql_scripts/testing')
sys.path.append('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/python_scripts/production')
from config.db_config import get_processor_db_connection
from ..production.utility_py_functions import *
import csv
from importlib import reload
from datetime import datetime, timedelta
#reload(load_sql_functions)
#reload(..production.utility_py_functions)

# File paths
csv_file_path = 'c:/Users/elan4/OneDrive/Documents/GitHub/equismart/config/testing_config/test_nse_stocks.csv'  # Update with your CSV file path
yaml_file_path = 'c:/Users/elan4/OneDrive/Documents/GitHub/equismart/config/testing_config/test_stock_filtering.yaml'  # Update with your YAML file path

# Input data
mode = 2
Stock_list = ['ASIANPAINT','AUROPHARMA'] # This list active only for the mode 3. Mode 3 is manual stock list for the summary generation.
Fetch_hist_data = 90 # in days
mov_avg = 60 #in days
Insertion_date = datetime.now().date() 
# Insertion_date = datetime.date(2024,12,26)



def read_stocks_from_csv(csv_file):
    df = pd.read_csv(csv_file, usecols=[0])  # Assuming no headers in the CSV
    
    df = df.dropna()
    #print(df)
    stock_list = df.iloc[:, 0].tolist()
    #stock_list = df[0].tolist() 
    #print(stock_list)
    #print(','.join(stock_list))
    return stock_list

def load_and_update_yaml(yaml_file, mode, mov_avg =60, stock_list=None, fetch_date=None, insert_date=None):
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)
        #print(stock_list)

    # Update YAML based on the mode
    for feature in yaml_data.get('features', []):
        if 'parameters' in feature:
            parameters = feature['parameters']
            parameters = parameters.replace("${mov_avg}", f"{mov_avg}")

            # Mode 0: All parameters updated to NULL
            if mode == 0:
                parameters = parameters.replace("${stocks}", "NULL")
                parameters = parameters.replace("${fetch_date}", "NULL")
                parameters = parameters.replace("${insert_date}", "NULL")

            # Mode 1: Only stocks parameter updated
            elif mode == 1 and stock_list is not None:
                stock_array = "{" + ",".join(stock_list) + "}"
                parameters = parameters.replace("${stocks}", f"'{stock_array}'")
                parameters = parameters.replace("${fetch_date}", "NULL")
                parameters = parameters.replace("${insert_date}", "NULL")

            # Mode 2: All parameters updated
            elif mode == 2 and stock_list is not None and fetch_date is not None and insert_date is not None:
                stock_array = "{" + ",".join(stock_list) + "}"
                parameters = parameters.replace("${stocks}", f"'{stock_array}'")
                parameters = parameters.replace("${fetch_date}", f"'{fetch_date}'")
                parameters = parameters.replace("${insert_date}", f"'{insert_date}'")

             # Mode 3: Manual Execution for few stocks
            elif mode == 3 and stock_list is not None:
                stock_array = "{" + ",".join(stock_list) + "}"
                parameters = parameters.replace("${stocks}", f"'{stock_array}'")
                parameters = parameters.replace("${fetch_date}", "NULL")
                parameters = parameters.replace("${insert_date}", "NULL")

            feature['parameters'] = parameters

    return yaml_data

if __name__ == "__main__":
    
    

    # Initialize variables
    stock_symbols = None
    fetch_date = None
    insert_date = None

    print("Execution starting....")
    print("Execution mode is",mode)
    # Step 1: Prepare data based on mode
    if mode in [1, 2]:
        stock_symbols = read_stocks_from_csv(csv_file_path)
    elif mode == 3:
        stock_symbols = Stock_list
    if mode == 2:
        insert_date = Insertion_date
        fetch_date = insert_date - timedelta(days=Fetch_hist_data)  # Fetch date is 90 days prior
        print('Summary Generation starting date:', insert_date)
        print('Raw Data hist fetch date:', fetch_date)

    
    

    # Step 2: Load and update YAML dynamically based on mode
    updated_yaml = load_and_update_yaml(yaml_file_path, mode, mov_avg, stock_symbols, fetch_date, insert_date)


    print("----Summary Generation start Timestamp:", pd.Timestamp.now())
    #yaml_config_path = 'c:/Users/elan4/OneDrive/Documents/GitHub/equismart/config/testing_config/test_stock_filtering.yaml'  # Path to your YAML config
    execute_summary_yaml(updated_yaml)
    print("----Summary Generation Finish Timestamp:", pd.Timestamp.now())

    # 