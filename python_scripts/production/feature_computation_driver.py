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
import argparse
#reload(load_sql_functions)
#reload(..production.utility_py_functions)

import time


#################################################################################################
# Global Parameters manually to be updated to execute the script directly from VS_Code. 
# Same set of parameters shall be updated by Jenkins under parse_arguments
#################################################################################################

#Insertion_date = datetime.now().date() 
Insertion_date = date(2025,3,14)
mov_avg = 60 #in days
Hibernation_Required = 'No'

Fetch_hist_data = 90 # in days
yaml_file_path = './config/production_config/fno_feature_computation.yaml'  # Update with your YAML file path

Manual_run = 'Yes'  # not modified by Jenkins but a decisional parameter for the direct execution from VS Code
# Stock_list = 'NIFTY,FINNIFY,BANKNIFTY'  # uncomment this line if specific list to be executed
Stock_list = ''  # Select 'Default' if you want to execute for sum_gen = Yes list other wise '' for complete execution

#################################################################################################

#################################################################################################
# Initialize required files for the computation. 
# Initialize global parameters not controlled by Jenkins
#################################################################################################

# File paths
Stock_primary_file_path = './config/production_config/NSE_Stocks_11.csv'  # Update with your CSV file path


# Input data
 # This list active only for the mode 3. Mode 3 is manual stock list for the summary generation.
# create Specific stock list CSV that can take 


#################################################################################################


def parse_arguments():
    global Insertion_date, mov_avg, Stock_list, Fetch_hist_data, yaml_file_path, Hibernation_Required, Manual_run
    print('inside parse_arguments()')

    # Check if command-line arguments are provided
    if len(sys.argv) > 1:
        Manual_run = 'No'
        parser = argparse.ArgumentParser(description='Process feature computation with specified parameters.')
        
        # Add arguments

        parser.add_argument('--yaml_path', type=str, help='Path to the YAML configuration file')
        parser.add_argument('--DB_Insert_date', type=str, help='Database insertion date (format: YYYY-MM-DD)')
        parser.add_argument('--stock_list', type=str, help='Comma-separated list of stocks')
        parser.add_argument('--mov_avg', type=int, help='Moving average period in days')
        parser.add_argument('--fetch_hist_data', type=int,  help='Fetch historical data in days')
        parser.add_argument('--Hibernation_Req', type=str, choices=['Yes', 'No'], help='Hibernation Required')

        
        
        
        args = parser.parse_args()

        # Update global variables if arguments are provided
        if args.yaml_path:
            yaml_file_path = args.yaml_path
        else:
            yaml_file_path = './config/production_config/fno_feature_computation.yaml'
        if args.DB_Insert_date:
            Insertion_date = datetime.strptime(args.DB_Insert_date, "%Y-%m-%d").date()
        else:
            Insertion_date = datetime.now().date() 
        if len(args.stock_list)!=0:
            Stock_list = args.stock_list
        else:
            Stock_list = ''
        if args.mov_avg:
            mov_avg = args.mov_avg
        else:
            mov_avg = 60
        if args.fetch_hist_data:
            Fetch_hist_data = args.fetch_hist_data
        else:
            Fetch_hist_data = 120


        if args.Hibernation_Req:
            Hibernation_Required = args.Hibernation_Req
        else:
            Hibernation_Required = 'Yes' 
    else:
        Manual_run = 'Yes'

def read_default_stock_list(csv_file):
    df = pd.read_csv(csv_file)  
    print(df)
    df = df.loc[df['Sum_Gen']=='Yes']
    
    df = df.dropna().reset_index(drop=True)
    print('number of stocks for feature computation is', len(df))
    print(df)
    stock_list = df.iloc[:, 0].tolist()
    print(stock_list)
    #print(','.join(stock_list))
    stock_list_csl = ",".join(stock_list)
    return stock_list_csl



def read_stocks_from_csv_start_stop(csv_file,start_index,stop_index):
    df = pd.read_csv(csv_file)  
    print(df)
    df = df.loc[df['Sum_Gen']=='Yes']
    
    df = df.dropna().reset_index(drop=True)
    print('number of stocks for feature computation is', len(df))
    print(df)

    if(start_index==0 and stop_index==0 ):
        stock_list = df.iloc[:, 0].tolist()
    else:
        stock_list = df.iloc[start_index:stop_index, 0].tolist()

    #stock_list = df[0].tolist() 
    print(stock_list)
    #print(','.join(stock_list))
    return stock_list

def load_and_update_yaml(yaml_file, mov_avg =60, stock_list_csl='', fetch_date=None, insert_date=None):

    if(len(stock_list_csl) == 0):
        stocks_parameter = "NULL"
    else:
        stock_array = "{" + stock_list_csl + "}"
        stocks_parameter = f"'{stock_array}'"

    if(fetch_date is None):
        fetch_date_parameter = "NULL"
    else:
        fetch_date_parameter = f"'{fetch_date}'"

    if(insert_date is None):
        insert_date_parameter = "NULL"
    else:
        insert_date_parameter = f"'{insert_date}'"

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
            parameters = parameters.replace("${stocks}", stocks_parameter)
            parameters = parameters.replace("${fetch_date}", fetch_date_parameter)
            parameters = parameters.replace("${insert_date}", insert_date_parameter)

            

            feature['parameters'] = parameters

    return yaml_data


def main ():
    global Insertion_date, mov_avg, Stock_list, Fetch_hist_data, yaml_file_path, Hibernation_Required, Manual_run

    if(len(Stock_list)!=0 and Stock_list == 'Default'):
        stock_symbols = read_default_stock_list(Stock_primary_file_path)
    elif(len(Stock_list)!=0 and Stock_list != 'Default'):
        stock_symbols = Stock_list
    else:
        stock_symbols = ''
    
    insert_date = Insertion_date
    fetch_date = insert_date - timedelta(days=Fetch_hist_data)

    updated_yaml = load_and_update_yaml(yaml_file_path, mov_avg, stock_symbols, fetch_date, insert_date)
    for feature in updated_yaml.get('features', []):
        function_name = feature['function_name']

    print(updated_yaml)
    print(f"----Feature computation of the {function_name} start Timestamp:", pd.Timestamp.now())
    execute_summary_yaml(updated_yaml)
    print(f"----Feature computation of the {function_name} Finish Timestamp:", pd.Timestamp.now())
    

if __name__ == "__main__":
    print('Entry to Main')
    parse_arguments()
    main()
    
if(Hibernation_Required == 'Yes'):
    print("System will hibernate in 1 min")
    time.sleep(60)
    print("Hibernating the System")
    os.system("shutdown /h")
else:
    print('No Hibernation')
