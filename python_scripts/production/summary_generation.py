import psycopg2
from psycopg2.extras import execute_values
import sys
import os
import pandas as pd
# sys.path.append('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/config')
# sys.path.append('c:/Users/elan4/OneDrive/Documents/GitHub/equismart')
# sys.path.append('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/sql_scripts/testing')
# sys.path.append('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/python_scripts/production')
from config.db_config import get_processor_db_connection
from ..production.db_interact_py_functions import *
import csv
from importlib import reload
from datetime import datetime, timedelta,date
#reload(load_sql_functions)
#reload(..production.utility_py_functions)


#################################################################################################
# Global Parameters manually to be updated to execute the script directly from VS_Code. 
# Same set of parameters shall be updated by Jenkins under parse_arguments
#################################################################################################

Insertion_date = datetime.now().date() 
#Insertion_date = date(2024,12,30)
mov_avg = 60 #in days
Hibernation_Required = 'No'
start_index = 0 # keep both of the indexes 0 for the complete execution
stop_index = 0
mode = 2  # Mode 2 is the production Mode

Fetch_hist_data = 120 # in days


Manual_run = 'Yes'  # not modified by Jenkins but a decisional parameter for the direct execution from VS Code


#################################################################################################

#################################################################################################
# Initialize required files for the computation. 
# Initialize global parameters not controlled by Jenkins
#################################################################################################

# File paths
csv_file_path = './config/production_config/NSE_Stocks_11.csv'  # Update with your CSV file path
yaml_file_path = './config/production_config/summary_generation.yaml'  # Update with your YAML file path

# Input data
Stock_list = ['NIFTY','FINNIFY','BANKNIFTY'] # This list active only for the mode 3. Mode 3 is manual stock list for the summary generation.
# create Specific stock list CSV that can take 


#################################################################################################






def read_stocks_from_csv(csv_file,start_index,stop_index):
    df = pd.read_csv(csv_file)  
    print(df)
    df = df.loc[df['Sum_Gen']=='Yes']
    
    df = df.dropna().reset_index(drop=True)
    print('number of stocks for Summary Generation is', len(df))
    print(df)

    if(start_index==0 and stop_index==0 ):
        stock_list = df.iloc[:, 0].tolist()
    else:
        stock_list = df.iloc[start_index:stop_index, 0].tolist()

    #stock_list = df[0].tolist() 
    print(stock_list)
    #print(','.join(stock_list))
    return stock_list

def load_and_update_yaml(yaml_file, mode, mov_avg =60, stock_list=None, fetch_date=None, insert_date=None):
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)
        #print(stock_list)

    # Update YAML based on the mode
    for feature in yaml_data.get('features', []):
        function_name = feature['function_name']
        print('Update starting for the function name:',function_name)
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
                if(function_name == 'compute_cm_summary'):
                    stock_array = "{" + ",".join(stock_list) + "}"
                    parameters = parameters.replace("${stocks}", f"'{stock_array}'")
                else:
                    parameters = parameters.replace("${stocks}", "NULL")
                parameters = parameters.replace("${fetch_date}", "NULL")
                parameters = parameters.replace("${insert_date}", "NULL")

            # Mode 2: All parameters updated
            elif mode == 2 and stock_list is not None and fetch_date is not None and insert_date is not None:
                if(function_name == 'compute_cm_summary'):
                    stock_array = "{" + ",".join(stock_list) + "}"
                    parameters = parameters.replace("${stocks}", f"'{stock_array}'")
                else:
                    parameters = parameters.replace("${stocks}", "NULL")

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
    if mode == 1 :
        stock_symbols = read_stocks_from_csv(csv_file_path,start_index,stop_index)
    elif mode == 3:
        stock_symbols = Stock_list
    elif mode == 2:
        stock_symbols = read_stocks_from_csv(csv_file_path,start_index,stop_index)
        insert_date = Insertion_date
        fetch_date = insert_date - timedelta(days=Fetch_hist_data)  # Fetch date is 90 days prior
        print('Summary Generation starting date:', insert_date)
        print('Raw Data hist fetch date:', fetch_date)

    
    

    # Step 2: Load and update YAML dynamically based on mode
    updated_yaml = load_and_update_yaml(yaml_file_path, mode, mov_avg, stock_symbols, fetch_date, insert_date)

    print(updated_yaml)
    print("----Summary Generation start Timestamp:", pd.Timestamp.now())
    #yaml_config_path = 'c:/Users/elan4/OneDrive/Documents/GitHub/equismart/config/testing_config/test_stock_filtering.yaml'  # Path to your YAML config
    #execute_summary_yaml(updated_yaml)
    print("----Summary Generation Finish Timestamp:", pd.Timestamp.now())

    # 