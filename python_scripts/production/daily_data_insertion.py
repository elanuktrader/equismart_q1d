import pyotp

import configparser
import pyotp
import argparse

# Load the configuration file
config = configparser.ConfigParser()
config.read('./config/user_config.ini')

# Fetch details
user = config['USER_DETAILS']['user']
pwd = config['USER_DETAILS']['pwd']
vc = config['USER_DETAILS']['vc']
app_key = config['USER_DETAILS']['app_key']
imei = config['USER_DETAILS']['imei']
token = config['USER_DETAILS']['token']
Raw_Loc = config['USER_DETAILS']['raw_loc']
factor2 = pyotp.TOTP(token).now()

from NorenRestApiPy.NorenApi import  NorenApi
from threading import Timer
import pandas as pd
import time
import concurrent.futures
import logging
import datetime
import numpy as np
from multiprocessing.pool import ThreadPool as Pool
import threading
#import schedule
#from FnO_Ranking import *
import os
import shutil
from time import sleep,perf_counter


import shutil

import sys
#from Volume_Profile import *
#sys.path.append('./config')
from config.db_config import get_processor_db_connection
from ..production.etl_functions import *



class ShoonyaApiPy(NorenApi):
    def __init__(self):
        #NorenApi.__init__(self, host='https://shoonyatrade.finvasia.com/NorenWClientTP/', websocket='wss://shoonyatrade.finvasia.com/NorenWSTP/', eodhost='https://shoonya.finvasia.com/chartApi/getdata/')
        #NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/', eodhost='https://api.shoonya.com/chartApi/getdata/')
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/')  
        global api
        api = self
         
 
#enable dbug to see request and responses
logging.basicConfig(level=logging.ERROR)

#start of our program
api = ShoonyaApiPy()

#make the api call
ret = api.login(userid=user, password=pwd, twoFA=factor2, vendor_code=vc, api_secret=app_key, imei=imei)

conn = get_processor_db_connection()
# Create a cursor object
cur = conn.cursor()





#################################################################################################
# Global Parameters manually to be updated to execute the script directly from VS_Code. 
# Same set of parameters shall be updated by Jenkins under parse_arguments
#################################################################################################


Raw_Start_Date = datetime.datetime(2025,8,1,9,0)
Raw_End_Date = datetime.datetime(2025,8,28,15,30)

# Raw_Start_Date = datetime.datetime(2024,12,30,9,0)
# Raw_End_Date = datetime.datetime(2024,12,31,15,30)

# Raw_Start_Date = datetime.datetime.today()
# Raw_Start_Date = Raw_Start_Date.replace(hour=9, minute=0, second=0, microsecond=0)
# Raw_End_Date = Raw_Start_Date.replace(hour=15, minute=30, second=0, microsecond=0)

Hibernation_Required = 'Yes'
Start_Index_1min = 363 # keep both of the indexes 0 for the complete execution
Stop_Index_1min = 0
Execution_Type = 'Schedule_Start' # 'Schedule_Start' to execute at scheduled time
                                  # 'Hard_Start' to execute immediately

Manual_run = 'Yes'  # not modified by Jenkins but a decisional parameter for the direct execution from VS Code


#################################################################################################



#################################################################################################
# Initialize required files for the computation. 
# Initialize global parameters not controlled by Jenkins
#################################################################################################


Stock_list_file_path = './config/production_config/NSE_Stocks_11.csv'
NFO_Name = 'NFO_symbols.txt.zip'
NFO_File = 'NFO_symbols.txt'

Stocks_list_DF = pd.read_csv(Stock_list_file_path)
print('Total Number of Stocks:', len(Stocks_list_DF))
Stocks_List = Stocks_list_DF.values.tolist()

Data_not_available_list = []
Operation_cm = 'Append'  # 'Create' or 'Append' Acquision operation. Mainly to create or append CSV nothing to do with SQL
Operation_fno = 'Append'
delay = 10

Schedule_time = datetime.datetime.today()
Schedule_time = Schedule_time.replace(hour=15, minute=45, second=0, microsecond=0)

insert_query_cm = """
    INSERT INTO raw_data.nse_stock_cm_data (nse_symbol, timestamp, open, high, low, close, vwap, volume, cum_vol)
    VALUES %s
    ON CONFLICT (nse_symbol, timestamp) DO NOTHING;
"""
insert_query_fno = """
    INSERT INTO raw_data.nse_stock_fno_data (nse_symbol, timestamp, open, high, low, close, vwap, volume, cum_vol,coi,oi,fut_series)
    VALUES %s
    ON CONFLICT (nse_symbol, timestamp,fut_series) DO NOTHING;
"""
#################################################################################################



#################################################################################################
# Compute global parameters not controlled by Jenkins
# These parameters are computed in the ETL_Functions.py
#################################################################################################

NFO_STK_FUT, NFO_IDX_FUT  = extract_NFO_scrip_master_data()

F1_Expiry,F2_Expiry,F3_Expiry = extract_Expiry_SM('NIFTY',NFO_IDX_FUT) 

BN_F1_Expiry,BN_F2_Expiry,BN_F3_Expiry = extract_Expiry_SM('BANKNIFTY',NFO_IDX_FUT)

FN_F1_Expiry,FN_F2_Expiry,FN_F3_Expiry = extract_Expiry_SM('FINNIFTY',NFO_IDX_FUT)

#################################################################################################

print('Finvasia Token is',factor2)

time.sleep(delay)





def parse_arguments():
    global Raw_Start_Date, Raw_End_Date, Start_Index_1min, Stop_Index_1min, Execution_Type, Hibernation_Required, Manual_run
    

    # Check if command-line arguments are provided
    if len(sys.argv) > 1:
        print('inside parse_arguments()')
        Manual_run = 'No'
        parser = argparse.ArgumentParser(description='Process stock data with specified parameters.')
        
        # Add arguments
        parser.add_argument('--Raw_Start_Date', type=str, help='Start date for raw data Acq (format: YYYY-MM-DDTHH:MM:SS)')
        parser.add_argument('--Raw_End_Date', type=str, help='End date for raw data Acq (format: YYYY-MM-DDTHH:MM:SS)')
        parser.add_argument('--Start_Index_1min', type=int, help='Start stock index for 1-minute data')
        parser.add_argument('--Stop_Index_1min', type=int, help='Stop stock index for 1-minute data')
        parser.add_argument('--Execution_Type', type=str, choices=['Hard_Start', 'Schedule_Start'], help='Execution type')
        parser.add_argument('--Hibernation_Req', type=str, choices=['Yes', 'No'], help='Hibernation Required')
        
        args = parser.parse_args()

        # Update global variables if arguments are provided
        if args.Raw_Start_Date:
            Raw_Start_Date = datetime.datetime.fromisoformat(args.Raw_Start_Date)
        else:
            Raw_Start_Date = datetime.datetime.now().replace(hour=9, minute=0, second=0)
        if args.Raw_End_Date:
            Raw_End_Date = datetime.datetime.fromisoformat(args.Raw_End_Date)
        else:
            Raw_End_Date = datetime.datetime.now().replace(hour=15, minute=30, second=0)
        if args.Start_Index_1min is not None:
            Start_Index_1min = args.Start_Index_1min
        else:
            Start_Index_1min = 0
        if args.Stop_Index_1min is not None:
            Stop_Index_1min = args.Stop_Index_1min
        else:
            Stop_Index_1min = 0
        if args.Execution_Type:
            Execution_Type = args.Execution_Type
        else:
            Execution_Type = 'Schedule_Start'

        if args.Hibernation_Req:
            Hibernation_Required = args.Hibernation_Req
        else:
            Hibernation_Required = 'Yes' 
    else:
        Manual_run = 'Yes'






def ETL_Stock_Data(Interval,Symbol,NSE_Name,Fut_Stock,Symbol_Token,Acq_req,Pros_Req,current_index):
    global Raw_Start_Date
    global Raw_End_Date
    Raw_Data_Path = Raw_Loc+"Min_"+str(Interval)+'/'
    Dest_File = Raw_Data_Path+Symbol+'.csv'
    
    
    

    print(Symbol,Symbol_Token)

    


    Symbol_Exch = 'NSE'
    Series =''
    
    if(Acq_req=='Yes'):
        Acq_Start_Time = perf_counter()
    
        Raw_Data_DF = extract_stock_raw_df(api,Symbol_Exch,Symbol_Token,Raw_Start_Date,Raw_End_Date,Interval)
        
        
        print('The length of Raw Data DF for',NSE_Name,len(Raw_Data_DF))    
        if(len(Raw_Data_DF)!=0):
            CSV_Stock_DF = transform_CSV_Stock_DF(Symbol_Exch,Raw_Data_DF,Series)
            SQL_Stock_DF = transform_SQL_Stock_DF(Symbol_Exch,Raw_Data_DF,NSE_Name,Series)
            load_cm_data(SQL_Stock_DF,NSE_Name,conn,insert_query_cm)
            
        
        
            if(Fut_Stock!='0'):
                
        
                print('Future Stock is:', NSE_Name)
                if(NSE_Name == 'BANKNIFTY'):
                    F1_Scrip = Fut_Stock+BN_F1_Expiry
                    F2_Scrip = Fut_Stock+BN_F2_Expiry
                    F3_Scrip = Fut_Stock+BN_F3_Expiry
                    print('inside if :',NSE_Name)
                    
                    
                elif(NSE_Name == 'FINNIFTY'):
                    F1_Scrip = Fut_Stock+FN_F1_Expiry
                    F2_Scrip = Fut_Stock+FN_F2_Expiry
                    F3_Scrip = Fut_Stock+FN_F3_Expiry
                    print('inside if :',NSE_Name)
                else:
                    F1_Scrip = Fut_Stock+F1_Expiry
                    F2_Scrip = Fut_Stock+F2_Expiry
                    F3_Scrip = Fut_Stock+F3_Expiry
                    #print('inside if :',NSE_Name)
                
                print('F1_Scrip',F1_Scrip,'F2_Scrip',F2_Scrip,'F3_Scrip',F3_Scrip)
                #Future 1 Details:
                Symbol_Token,Symbol_Exch = extract_fno_scripdetails(F1_Scrip,NSE_Name,NFO_STK_FUT,NFO_IDX_FUT)
                
                Operaion_fno = 'Create'
                F1_Raw_DF = extract_stock_raw_df(api,Symbol_Exch,Symbol_Token,Raw_Start_Date,Raw_End_Date, Interval)
                if(len(F1_Raw_DF)!=0):
                    Series = 'F1_'
                    F1_DF = transform_CSV_Stock_DF(Symbol_Exch,F1_Raw_DF,Series)
                    F1_Loc = Raw_Data_Path+NSE_Name+'_F1.csv'
                    print('---------------------------------------')
                    #load_CSV_Stock_DF(F1_DF,Operation,F1_Loc)
                    load_CSV_Stock_DF(F1_DF,Operation_fno,F1_Loc)

                    SQL_F1_DF = transform_SQL_Stock_DF(Symbol_Exch,F1_Raw_DF,NSE_Name,Series)
                    
                    #Future 2 Details:
                    Symbol_Token,Symbol_Exch = extract_fno_scripdetails(F2_Scrip,NSE_Name,NFO_STK_FUT,NFO_IDX_FUT)
                    Series = 'F2_'
                    F2_Raw_DF = extract_stock_raw_df(api,Symbol_Exch,Symbol_Token,Raw_Start_Date,Raw_End_Date, Interval)
                    if(len(F2_Raw_DF)!=0):
                        F2_DF = transform_CSV_Stock_DF(Symbol_Exch,F2_Raw_DF,Series)
                        F2_Loc = Raw_Data_Path+NSE_Name+'_F2.csv'
                        print('---------------------------------------')
                        #load_CSV_Stock_DF(F2_DF,Operation,F2_Loc)
                        load_CSV_Stock_DF(F2_DF,Operation_fno,F2_Loc)

                        SQL_F2_DF = transform_SQL_Stock_DF(Symbol_Exch,F2_Raw_DF,NSE_Name,Series)
                    else:
                        print('No data for',F2_Scrip)
                        F2_DF = F1_DF.copy(deep=True)
                        SQL_F2_DF = pd.DataFrame()
                    
                    #Future 3 Details:
                    Symbol_Token,Symbol_Exch = extract_fno_scripdetails(F3_Scrip,NSE_Name,NFO_STK_FUT,NFO_IDX_FUT)
                    Series = 'F3_'
                    F3_Raw_DF = extract_stock_raw_df(api,Symbol_Exch,Symbol_Token,Raw_Start_Date,Raw_End_Date, Interval)

                    if(len(F3_Raw_DF)!=0):

                        F3_DF = transform_CSV_Stock_DF(Symbol_Exch,F3_Raw_DF,Series)
                        F3_Loc = Raw_Data_Path+NSE_Name+'_F3.csv'
                        print('---------------------------------------')
                        #load_CSV_Stock_DF(F3_DF,Operation,F3_Loc)
                        load_CSV_Stock_DF(F3_DF,Operation_fno,F3_Loc)

                        SQL_F3_DF = transform_SQL_Stock_DF(Symbol_Exch,F3_Raw_DF,NSE_Name,Series)
                    else:
                        print('No data for',F3_Scrip)
                        F3_DF = F2_DF.copy(deep=True)
                        SQL_F3_DF = pd.DataFrame()


                    

                    
                    CSV_Stock_DF = Merge_Future_Data(CSV_Stock_DF,F1_DF,F2_DF,F3_DF)
                    load_fno_data(SQL_F1_DF,SQL_F2_DF,SQL_F3_DF,NSE_Name,conn,insert_query_fno)


                else:
                    print('Future Data is unavaiable for',NSE_Name)
                    Data_not_available_list.append({"Name": NSE_Name, "Exchange": 'NFO', "Index":current_index})
                
                
                
                    #print(F3_DF)
        
            load_CSV_Stock_DF(CSV_Stock_DF,Operation_cm,Dest_File)
        else:
            print('NSE Data is unavaiable for',NSE_Name)
            
            Data_not_available_list.append({"Name": NSE_Name, "Exchange": 'NSE', "Index":current_index})

            
        Acq_End_Time = perf_counter()
        print(f'It took {Acq_End_Time- Acq_Start_Time: 0.2f} second(s) to Acquire:',Symbol,'with Interval:',Interval)



def ETL_Initiate(Interval,Start_Index,Acq_Req,Pros_Req,Stop_Index):
    global Stocks_List
    #global Start_Index
    
    print('Time Interval is:',Interval)
    
    
    
    print(len(Stocks_List))
    if(Stop_Index==0):
        Stop_Index = len(Stocks_List)
    
    if(Start_Index<=Stop_Index):
        for i in range(Stop_Index):
            
            
            if (i>=Start_Index and i < Stop_Index):
                current_index = i
                Symbol = Stocks_List[current_index][1]
                NSE_Name = Stocks_List[current_index][0]
                Fut_Stock = Stocks_List[current_index][2]
                Symbol_Token = str(int(Stocks_List[current_index][5]))
                print('=====================================================================')
                print('Current Index',current_index, '|| Interval',Interval, '|| Stock:',NSE_Name)
                ETL_Stock_Data(Interval,Symbol,NSE_Name,Fut_Stock,Symbol_Token,Acq_Req,Pros_Req,current_index)
                

    


def Launch_Execution():
    print('Execution starting.....')
    time.sleep(delay)
    start_time = perf_counter()
    ETL_Initiate(1,Start_Index_1min,'Yes','No',Stop_Index_1min)
    end_time = perf_counter()
        
    print(f'It took {end_time- start_time: 0.2f} second(s) to complete Acquision and Database update.')

        






def main():
    print('inside main()')
    global Raw_Start_Date, Raw_End_Date, Start_Index_1min, Stop_Index_1min, Execution_Type, Hibernation_Required, Manual_run,delay


    # Print the parameters being used
    print('=====================================================================')
    print(f"Raw Start Date: {Raw_Start_Date}")
    print(f"Raw End Date: {Raw_End_Date}")
    print('=====================================================================')
    print(f"Start Stock Index: {Start_Index_1min}")
    print(f"Stop Stock Index: {Stop_Index_1min} Zero here indicates complete list")
    print("First Stock in the Stocks List is:", Stocks_List[0][0])
    print('Execution Starting Stock:',Stocks_List[Start_Index_1min][0])    
    print('=====================================================================')
    print(f"Execution Type: {Execution_Type}")
    print(f"Hibernation Required: {Hibernation_Required}")
    print(f"Manual Run: {Manual_run}")
    

    if(Execution_Type == 'Schedule_Start' ):
        delay=10
    else:
        delay = 1

    time.sleep(delay)

    print('=====================================================================')

    print('Operational settings')
    print('Acquision operation for Cash Market is:',Operation_cm)
    print('Acquision operation for F&O is:',Operation_fno)
    print('=====================================================================')

    if(Start_Index_1min!=0 and Manual_run == 'Yes'):
        print("Partial Execution - Be mindful   Verify above Execution Starting Stock")
        Partial_Execution = input ("To proceed further enter 'y': or Hit Stop button: ")
        print("Proceeding with Partial Execution")
    
    else:
        print("Starting execution for the stock index", Start_Index_1min)

    

    if(Execution_Type == 'Schedule_Start'):
        print('This is a scheduled Execution and Execution will launch at a specified time ')
        Current_time = datetime.datetime.today()
        Current_time = Current_time.replace(second=0, microsecond=0)
        print('Current Time is', Current_time)

        print("Scheduled Time is", Schedule_time)
        delta_time = Schedule_time-Current_time
        delta_time_secs = int(delta_time.total_seconds())
        
        
        
        if(delta_time_secs>0):
            print('Execution Starts in',delta_time,' or', delta_time_secs, 'Seconds')
            time.sleep(delta_time_secs)
        else:
            print('Time lapsed already. Execution starting in 10 seconds')
            time.sleep(delay)
        
        
        Launch_Execution()
    
    
    elif(Execution_Type == 'Hard_Start'):
        print("This is Hard execution. You may be executing this is in the trading hours. This could lead to corruption of data")

        if (Manual_run == 'Yes'):
            Partial_Execution = input ("To proceed further enter 'y': or Hit Stop button: ")
        print('This is a hard execution Launching Execution now')
        Launch_Execution()

if __name__ == '__main__':
    print('Entry to Main')
    parse_arguments()
    main()



os.remove(NFO_Name)
os.remove(NFO_File)
generate_data_unavail_report(Data_not_available_list,Start_Index_1min,Stop_Index_1min)


if(Hibernation_Required == 'Yes'):
    print("System will hibernate in 1 min")
    time.sleep(60)
    print("Hibernating the System")
    os.system("shutdown /h")
else:
    print('No Hibernation')