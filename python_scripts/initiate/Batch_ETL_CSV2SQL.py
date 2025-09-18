from threading import Timer
import pandas as pd
import time
import concurrent.futures
import logging
import datetime
import numpy as np
import threading
import schedule
import os
import shutil
from time import sleep,perf_counter
from urllib import request
import zipfile
import shutil
import psycopg2
from psycopg2.extras import execute_values
import sys
sys.path.append('c:\\Users\\elan4\\OneDrive\\Documents\\GitHub\\equismart\\config')
from config.db_config import get_processor_db_connection

Stock_list_file_path = 'c:/Users/elan4/OneDrive/Documents/GitHub/equismart/config/production_config/NSE_Stocks_10.csv'
Stocks_list_DF = pd.read_csv(Stock_list_file_path)
Finv_Raw_Loc = "E:\\Trading\\Finvasia_API\\Daily\\Raw_Data\\"
Finvasia_Start_Date = datetime.date(2024,11,29)
Interval =1
print('Time Interval is:',Interval)
Start_Index= 0
Stop_Index = 0
Stocks_List = Stocks_list_DF.values.tolist()

print(len(Stocks_List))
Single_insert_size= 10
i = Start_Index
Data_not_available = []
conn = get_processor_db_connection()
cur = conn.cursor()

insert_query_fno = """
    INSERT INTO raw_data.nse_stock_fno_data (nse_symbol, timestamp, open, high, low, close, vwap, volume, cum_vol,coi,oi,fut_series)
    VALUES %s
    ON CONFLICT (nse_symbol, timestamp,fut_series) DO NOTHING;
"""

# Define the insert query with ON CONFLICT clause to handle duplicates
insert_query_cm = """
    INSERT INTO raw_data.nse_stock_cm_data (nse_symbol, timestamp, open, high, low, close, vwap, volume, cum_vol)
    VALUES %s
    ON CONFLICT (nse_symbol, timestamp) DO NOTHING;
"""



def rename_columns(Scrip_DF,Series):
    
    
    prefix = Series+"_"
        
    # Find columns that start with the given prefix
    columns_to_update = [col for col in Scrip_DF.columns if col.startswith(prefix)]
    
    # Update column names dynamically
    Scrip_DF.rename(columns={col: col.replace(prefix, "") for col in columns_to_update}, inplace=True)
    
    # print(Scrip_DF.columns)
    return Scrip_DF

def Data_Type_Conversion_CM(Scrip_DF_local):
    Scrip_DF_local.reset_index(drop=True, inplace=True)
    
    Scrip_DF_local['Timestamp'] = pd.to_datetime(Scrip_DF_local['Timestamp'])  # Convert to datetime
    #Scrip_DF['Date'] = Scrip_DF['Timestamp'].dt.date               # Extract date
    #Scrip_DF['Time'] = Scrip_DF['Timestamp'].dt.time               # Extract time
    Scrip_DF_local['Open'] = Scrip_DF_local['Open'].astype(float)       # Ensure float
    Scrip_DF_local['High'] = Scrip_DF_local['High'].astype(float)       # Ensure float
    Scrip_DF_local['Low'] = Scrip_DF_local['Low'].astype(float)         # Ensure float
    Scrip_DF_local['Close'] = Scrip_DF_local['Close'].astype(float)     # Ensure float
    Scrip_DF_local['VWAP'] = Scrip_DF_local['VWAP'].astype(float)       # Ensure float
    Scrip_DF_local['Volume'] = Scrip_DF_local['Volume'].astype(int)     # Ensure integer
    Scrip_DF_local['Cum_Vol'] = Scrip_DF_local['Cum_Vol'].astype(int)   # Ensure integer
    Scrip_DF_local.columns = Scrip_DF_local.columns.str.lower()
    #print(Scrip_DF_local.columns)
    valid_df = Scrip_DF_local.loc[
                     (Scrip_DF_local["open"] > 0) &
                     (Scrip_DF_local["high"] > 0) &
                     (Scrip_DF_local["low"] > 0) &
                     (Scrip_DF_local["close"] > 0) &
                     (Scrip_DF_local["vwap"] >= 0) &
                     (Scrip_DF_local["volume"] >= 0) &
                     (Scrip_DF_local["cum_vol"] >= 0)
                 ]
    
    
    
    return valid_df

def Data_Type_Conversion_FNO(Scrip_DF_local):
    Scrip_DF_local.reset_index(drop=True, inplace=True)
    
    Scrip_DF_local['Timestamp'] = pd.to_datetime(Scrip_DF_local['Timestamp'])  # Convert to datetime
    #Scrip_DF['Date'] = Scrip_DF['Timestamp'].dt.date               # Extract date
    #Scrip_DF['Time'] = Scrip_DF['Timestamp'].dt.time               # Extract time
    Scrip_DF_local['Open'] = Scrip_DF_local['Open'].astype(float)       # Ensure float
    Scrip_DF_local['High'] = Scrip_DF_local['High'].astype(float)       # Ensure float
    Scrip_DF_local['Low'] = Scrip_DF_local['Low'].astype(float)         # Ensure float
    Scrip_DF_local['Close'] = Scrip_DF_local['Close'].astype(float)     # Ensure float
    Scrip_DF_local['VWAP'] = Scrip_DF_local['VWAP'].astype(float)       # Ensure float
    Scrip_DF_local['Volume'] = Scrip_DF_local['Volume'].astype('int32')     # Ensure integer
    Scrip_DF_local['Cum_Vol'] = Scrip_DF_local['Cum_Vol'].astype('int32')   # Ensure integer
    Scrip_DF_local['COI'] = Scrip_DF_local['COI'].fillna(0).astype('int32')     # Ensure integer
    Scrip_DF_local['OI'] = Scrip_DF_local['OI'].astype('int32')   # Ensure integer
    Scrip_DF_local['fut_series'] = Scrip_DF_local['fut_series'].astype('string')
    Scrip_DF_local.columns = Scrip_DF_local.columns.str.lower()
    #print(Scrip_DF_local.columns)
    valid_df = Scrip_DF_local.loc[
                    (Scrip_DF_local["open"] > 0) &
                    (Scrip_DF_local["high"] > 0) &
                    (Scrip_DF_local["low"] > 0) &
                    (Scrip_DF_local["close"] > 0) &
                    (Scrip_DF_local["volume"] >= 0) &
                    (Scrip_DF_local["vwap"] >= 0) &
                    (Scrip_DF_local["cum_vol"] >= 0) &
                    (Scrip_DF_local["oi"] >= 0)
                ]
    
    return valid_df


def Get_Finvasia_FNO_Data(Symbol,NSE_Name,Raw_Loc,Start_Date,Series):
    Raw_Data_Path = Raw_Loc+"Min_"+str(Interval)+'/'
    Dest_File = Raw_Data_Path+NSE_Name+'_'+Series+'.csv'
    try:
        
        Scrip_DF = pd.read_csv(Dest_File)

            
        Scrip_DF.rename(columns={'DT': 'Timestamp'}, inplace=True)
        Scrip_DF['Timestamp'] = pd.to_datetime(Scrip_DF['Timestamp'])
        #Scrip_DF.reset_index(drop=False, inplace=True)
        Scrip_DF['NSE_Symbol'] = NSE_Name
        Scrip_DF['fut_series'] = Series
        Scrip_DF['Time'] = pd.to_datetime(Scrip_DF['Timestamp']).dt.time
        Scrip_DF = Scrip_DF.loc[(Scrip_DF['Time']>datetime.time(9,15)) & (Scrip_DF['Time']<=datetime.time(15,29))]
        Scrip_DF = rename_columns(Scrip_DF,Series)
        Scrip_DF = Scrip_DF[['NSE_Symbol', 'Timestamp', 'Open', 'High', 'Low', 'Close', 'VWAP', 'Volume', 'Cum_Vol','COI','OI','fut_series']]
        
        Scrip_DF = Scrip_DF.loc[(Scrip_DF['Timestamp'].dt.date >= Start_Date)]  
        Scrip_DF = Scrip_DF.loc[(Scrip_DF['Volume']>=0)] 
        #Scrip_DF['Volume'] = Scrip_DF['Volume'].fillna(0)
        
        Latest_data_indicator = len(Scrip_DF.loc[(Scrip_DF['Timestamp'].dt.date >= Start_Date)]  )
        
        Scrip_DF.reset_index(drop=True, inplace=True)
        
    except:
        Scrip_DF = pd.DataFrame()
        Latest_data_indicator = 0
        print('Check the following file',Dest_File)
    return Scrip_DF,Latest_data_indicator

def Get_Finvasia_CM_Data(Symbol,NSE_Name,Raw_Loc,Start_Date):
    Raw_Data_Path = Raw_Loc+"Min_"+str(Interval)+'/'
    Dest_File = Raw_Data_Path+Symbol+'.csv'
    try:
        Scrip_DF = pd.read_csv(Dest_File,index_col=0)
        Scrip_DF.rename(columns={'DT': 'Timestamp'}, inplace=True)
        Scrip_DF['Timestamp'] = pd.to_datetime(Scrip_DF['Timestamp'])
        #Scrip_DF.reset_index(drop=False, inplace=True)
        Scrip_DF['NSE_Symbol'] = NSE_Name
        Scrip_DF['Time'] = pd.to_datetime(Scrip_DF['Timestamp']).dt.time
        Scrip_DF = Scrip_DF.loc[(Scrip_DF['Time']>=datetime.time(9,15)) & (Scrip_DF['Time']<=datetime.time(15,29))]
        Scrip_DF = Scrip_DF[['NSE_Symbol', 'Timestamp', 'Open', 'High', 'Low', 'Close', 'VWAP', 'Volume', 'Cum_Vol']]
        
        Scrip_DF = Scrip_DF.loc[(Scrip_DF['Timestamp'].dt.date >= Start_Date)]  
        Scrip_DF = Scrip_DF.loc[(Scrip_DF['Volume']>=0)] 
        #Scrip_DF['Volume'] = Scrip_DF['Volume'].fillna(0)
        
        Latest_data_indicator = len(Scrip_DF.loc[(Scrip_DF['Timestamp'].dt.date >= Start_Date)])
        
        Scrip_DF.reset_index(drop=True, inplace=True)
        
    except:
        Scrip_DF = pd.DataFrame()
        Latest_data_indicator = 0
        print('Check the following file',Dest_File)
    return Scrip_DF,Latest_data_indicator


if __name__ == "__main__":
    print("----Execution start Timestamp:", pd.Timestamp.now())
    if(Stop_Index == 0):
        Stop_Index =  len(Stocks_List)

    if(Start_Index<=Stop_Index):
        while (i<Stop_Index):
            
            c1_start = perf_counter()
            if (i>=Start_Index and i< Stop_Index):
                print('-----------------------------------------------------------')
                print(f"Starting Index: {i}")
                All_Stock_CM_DF = pd.DataFrame()
                All_Stock_FNO_DF = pd.DataFrame() 
                
                for j in range (Single_insert_size):
                    combi_index = i+j
                    if(combi_index<Stop_Index):
                    
                        Symbol = Stocks_List[combi_index][1]
                        NSE_Name = Stocks_List[combi_index][0]
                        Fut_Stock = Stocks_List[combi_index][2]
                        Sum_Gen_Flag = Stocks_List[combi_index][10]
                        
                        if(Sum_Gen_Flag == 'Yes'):
                            Finvasia_CM_DF,Latest_CM_Data_present = Get_Finvasia_CM_Data(Symbol,NSE_Name,Finv_Raw_Loc,Finvasia_Start_Date)
                            print('Processing starting: Inner Loop Index for the Stock',NSE_Name,i+j)
                            
                            
                            if(len(Finvasia_CM_DF)!=0):
                                CM_Stock_DF = Finvasia_CM_DF
                            else:
                                Latest_CM_Data_present = 0
                                Data_not_available.append(NSE_Name)
                                CM_Stock_DF = pd.DataFrame()
                            
                                

                            if(Latest_CM_Data_present!=0):
                                if(j==0):
                                    All_Stock_CM_DF = CM_Stock_DF   
                                else:
                                    All_Stock_CM_DF =pd.concat([All_Stock_CM_DF, CM_Stock_DF], axis=0, ignore_index=True)
                            else:
                                print('No Latest Data found for',NSE_Name)

                            if(Fut_Stock!='0'):
                                print('FnO Processing starting: for the Stock',NSE_Name,i+j)
                                Finvasia_DF3,Latest_F3_Data_present = Get_Finvasia_FNO_Data(Symbol,NSE_Name,Finv_Raw_Loc,Finvasia_Start_Date,'F3')
                                Finvasia_DF2,Latest_F2_Data_present = Get_Finvasia_FNO_Data(Symbol,NSE_Name,Finv_Raw_Loc,Finvasia_Start_Date,'F2')
                                Finvasia_DF1,Latest_F1_Data_present = Get_Finvasia_FNO_Data(Symbol,NSE_Name,Finv_Raw_Loc,Finvasia_Start_Date,'F1')

                                if(Latest_F1_Data_present!=0):
                                    Finvasia_FNO_DF = pd.concat([Finvasia_DF1, Finvasia_DF2, Finvasia_DF3], axis=0, ignore_index=True)


                                if(Latest_F1_Data_present!=0):
                                    if(j==0):
                                        All_Stock_FNO_DF = Finvasia_FNO_DF  
                                    else:
                                        All_Stock_FNO_DF =pd.concat([All_Stock_FNO_DF, Finvasia_FNO_DF], axis=0, ignore_index=True)
                                else:
                                    print('No Latest Data found for',NSE_Name)


                        
                    
                print('Stock insertion for the batch index',i,i+j)
                print('Total number of rows',len(All_Stock_CM_DF))
                #print(All_Stock_DF.columns)
                #All_Stock_DF.to_csv(NSE_Name+'.csv',index=False)
                Valid_CM_DF = Data_Type_Conversion_CM(All_Stock_CM_DF)
                
                
                
                #All_Stock_DF.to_csv(NSE_Name+'.csv',index=False)
                
                    # Convert the DataFrame to a list of tuples for insertion
                data_tuples_cm = list(Valid_CM_DF.itertuples(index=False, name=None))
                
                #valid_df.to_csv(NSE_Name+'.csv')
                print('Total number of rows CM after filtering',len(Valid_CM_DF))
                print('Total number of rows CM after duplicates removal',len(Valid_CM_DF.drop_duplicates(subset=['nse_symbol', 'timestamp'])))

                if(len(All_Stock_FNO_DF)!=0):
                    Valid_FNO_DF = Data_Type_Conversion_FNO(All_Stock_FNO_DF)
                    data_tuples_fno = list(Valid_FNO_DF.itertuples(index=False, name=None))

                    print('Total number of rows FNO after filtering',len(Valid_FNO_DF))
                    print('Total number of rows FNO after duplicates removal',len(Valid_FNO_DF.drop_duplicates(subset=['nse_symbol', 'timestamp','fut_series'])))
                else:
                    print('No FNO data in this inner loop')
            
                c1_end = perf_counter()
                # Insert data into the PostgreSQL table
                i=i+j+1
                try:

                    
                    # Print the local timestamp
                    print("----Database insertion start Timestamp:", pd.Timestamp.now())
                    c2_start = perf_counter()
                    Valid_CM_DF.to_csv('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/test_output/'+NSE_Name+'.csv')
                    execute_values(cur, insert_query_cm, data_tuples_cm)
                    conn.commit()

                    if(len(All_Stock_FNO_DF)!=0):
                        Valid_FNO_DF.to_csv('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/test_output/'+NSE_Name+'_fno'+'.csv')
                        execute_values(cur, insert_query_fno, data_tuples_fno)
                        conn.commit()

                    c2_end = perf_counter()
                    print(f"Data group from {i} to {i+j} has been inserted into the database.")
                    print(f"Data Processing time : {round(c1_end-c1_start,2)} Database Insersion Time: {round(c2_end-c2_start,2)}")
                    print("----Database insertion Finish Timestamp:", pd.Timestamp.now())
                except Exception as e:
                    conn.rollback()
                    print(f"Error inserting data from {NSE_Name}: {e}")
            #else:
                #print('No Latest data available for',NSE_Name)
    print("----Execution stop Timestamp:", pd.Timestamp.now())
    pd.DataFrame(Data_not_available).to_csv('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/test_output/'+'data_unavailable.csv')
# Close the connection
cur.close()
conn.close()
