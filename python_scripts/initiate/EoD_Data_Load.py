import pandas as pd
import time
from datetime import datetime
import numpy as np
import threading
import schedule
import os
import shutil
from time import sleep,perf_counter
from urllib import request
import zipfile
import psycopg2
from psycopg2.extras import execute_values
import getpass
import traceback

# PostgreSQL connection parameters
dbname='NSE_Stock_Data'
user='postgres'
password='EquiSmart24'
host='localhost'
port='5432'

### Assign One among : CM_Bhav, New_CM_Bhav, FnO_Bhav, New_FnO_Bhav, Daily_Vol, Participant_OI, Participant_Vol
Category = 'New_CM_Bhav'

### Accordingly the base folder
Folder_Path = 'E:\\Trading\\Swing\\Data\\NSE_CM_Bhav\\'
Sub_Folder_Names = ['2017','2018', '2019', '2020', '2021', '2022', '2023','2024','2025']
#Sub_Folder_Names = ['2024','2025']

# Set up the database connection
conn = psycopg2.connect(
        dbname='NSE_Stock_Data',
        user='postgres',
        password='EquiSmart24',
        host='localhost',
        port='5432'
    )
DATABASE = dbname
# Create a cursor object
cur = conn.cursor()

def list_bhav_files_in_folder (folder_path):
    
    # List all files and directories
    files_and_dirs = os.listdir(folder_path)
    
    # Filter out only .csv files
    csv_files = [f for f in files_and_dirs if f.endswith('bhav.csv') and os.path.isfile(os.path.join(folder_path, f))]
    
    return csv_files

def list_new_bhav_files_in_folder (folder_path):
    
    # List all files and directories
    files_and_dirs = os.listdir(folder_path)
    
    # Filter out only .csv files
    csv_files = [f for f in files_and_dirs if f.startswith('sec_bhavdata_full_') and os.path.isfile(os.path.join(folder_path, f))]
    
    return csv_files

def list_new_fno_bhav_files_in_folder (folder_path):
    
    # List all files and directories
    files_and_dirs = os.listdir(folder_path)
    
    # Filter out only .csv files
    csv_files = [f for f in files_and_dirs if f.endswith('_F_0000.csv.zip') and os.path.isfile(os.path.join(folder_path, f))]
    
    return csv_files

def list_csv_files_in_folder (folder_path):
    
    # List all files and directories
    files_and_dirs = os.listdir(folder_path)
    
    # Filter out only .csv files
    csv_files = [f for f in files_and_dirs if f.endswith('.csv') and os.path.isfile(os.path.join(folder_path, f))]
    
    return csv_files

def list_vol_files_in_folder (folder_path):
    
    # List all files and directories
    files_and_dirs = os.listdir(folder_path)
    
    # Filter out only .csv files
    vol_files = [f for f in files_and_dirs if f.endswith('.csv') and f.startswith('fao_participant_vol_') and os.path.isfile(os.path.join(folder_path, f))]
    
    return vol_files

def list_oi_files_in_folder (folder_path):
    
    # List all files and directories
    files_and_dirs = os.listdir(folder_path)
    
    # Filter out only .csv files
    oi_files = [f for f in files_and_dirs if f.endswith('.csv') and f.startswith('fao_participant_oi_') and os.path.isfile(os.path.join(folder_path, f))]
    
    return oi_files

def list_dat_files_in_folder (folder_path):
    
    # List all files and directories
    files_and_dirs = os.listdir(folder_path)
    
    # Filter out only .csv files
    dat_files = [f for f in files_and_dirs if f.endswith('.DAT') and os.path.isfile(os.path.join(folder_path, f))]
    
    return dat_files

def FnO_Bhav_Data (Scrip_DF):
    
    # There is a empty column from source file    
    Scrip_DF = Scrip_DF.dropna (axis=1, how='all')
    # print (len(Scrip_DF))

    Scrip_DF = Scrip_DF.loc [Scrip_DF['INSTRUMENT'].str.contains('FUT'), ['INSTRUMENT','SYMBOL','EXPIRY_DT','OPEN','HIGH','LOW','CLOSE','SETTLE_PR','CONTRACTS','VAL_INLAKH','OPEN_INT','CHG_IN_OI','TIMESTAMP']]

    Scrip_DF['TIMESTAMP'] = Scrip_DF['TIMESTAMP'].dt.date  # Convert to datetime       
    Scrip_DF['EXPIRY_DT'] = Scrip_DF['EXPIRY_DT'].dt.date  # Convert to datetime       
    Scrip_DF['OPEN'].apply (lambda x : float(x)),
    Scrip_DF['HIGH'].apply (lambda x : float(x)),
    Scrip_DF['LOW'].apply (lambda x : float(x)),
    Scrip_DF['CLOSE'].apply (lambda x : float(x)),
    Scrip_DF['SETTLE_PR'].apply (lambda x : float(x)),
    Scrip_DF['CONTRACTS'].apply (lambda x : int(x)),
    Scrip_DF['VAL_INLAKH'].apply (lambda x : float(x)),
    Scrip_DF['OPEN_INT'].apply (lambda x : int(x)),
    Scrip_DF['CHG_IN_OI'].apply (lambda x : int(x)),

    return Scrip_DF

def New_FnO_Bhav_Data (Scrip_DF):
    
    Scrip_DF = Scrip_DF.loc [Scrip_DF['FinInstrmTp'].isin (['IDF', 'STF']), ['FinInstrmTp','TckrSymb','XpryDt','OpnPric','HghPric','LwPric','ClsPric','SttlmPric','TtlTradgVol','TtlTrfVal','OpnIntrst','ChngInOpnIntrst','TradDt', 'UndrlygPric']]
    # print (len(Scrip_DF))

    Scrip_DF['TradDt'] = Scrip_DF['TradDt'].dt.date  # Convert to datetime       
    Scrip_DF['XpryDt'] = Scrip_DF['XpryDt'].dt.date  # Convert to datetime       
    Scrip_DF['OpnPric'].apply (lambda x : float(x)),
    Scrip_DF['HghPric'].apply (lambda x : float(x)),
    Scrip_DF['LwPric'].apply (lambda x : float(x)),
    Scrip_DF['ClsPric'].apply (lambda x : float(x)),
    Scrip_DF['SttlmPric'].apply (lambda x : float(x)),
    Scrip_DF['TtlTradgVol'].apply (lambda x : int(x)),
    Scrip_DF['TtlTrfVal'].apply (lambda x : float(x)),
    Scrip_DF['OpnIntrst'].apply (lambda x : int(x)),
    Scrip_DF['ChngInOpnIntrst'].apply (lambda x : int(x)),
    Scrip_DF['UndrlygPric'].apply (lambda x : float(x)),

    return Scrip_DF

def Participant_Data (Scrip_DF):
    
    # print (Scrip_DF)
    # There is a empty column from source file    
    Scrip_DF.columns = Scrip_DF.columns.str.rstrip ()
    # print (Scrip_DF)

    Scrip_DF['DATE'] = Scrip_DF['DATE'].dt.date  # Convert to datetime       

    Scrip_DF['Future Index Long'].apply (lambda x : int(x))
    Scrip_DF['Future Index Short'].apply (lambda x : int(x))
    Scrip_DF['Future Stock Long'].apply (lambda x : int(x))
    Scrip_DF['Future Stock Short'].apply (lambda x : int(x))
    Scrip_DF['Option Index Put Long'].apply (lambda x : int(x))
    Scrip_DF['Option Index Call Short'].apply (lambda x : int(x))
    Scrip_DF['Option Index Put Short'].apply (lambda x : int(x))
    Scrip_DF['Option Stock Call Long'].apply (lambda x : int(x))
    Scrip_DF['Option Stock Put Long'].apply (lambda x : int(x))
    Scrip_DF['Option Stock Call Short'].apply (lambda x : int(x))
    Scrip_DF['Option Stock Put Short'].apply (lambda x : int(x))
    Scrip_DF['Total Long Contracts'].apply (lambda x : int(x))
    Scrip_DF['Total Short Contracts'].apply (lambda x : int(x))    
    
    return Scrip_DF

def Daily_Vol_Data (Scrip_DF):
    
    # print (Scrip_DF)
    # There is a empty column from source file
    Scrip_DF = Scrip_DF.dropna (axis=1, how='all')
    # print (Scrip_DF)

    Scrip_DF['TIMESTAMP'] = Scrip_DF['TIMESTAMP'].dt.date  # Convert to datetime        
    Scrip_DF['SR_NO'].apply (lambda x : int(x))       # Ensure int
    Scrip_DF['QTY_TRD'].apply (lambda x : int(x))         # Ensure float
    Scrip_DF['DEL_QTY'].apply (lambda x : int(x))     # Ensure int
    Scrip_DF['DEL_%'].apply (lambda x : float(x))       # Ensure float
    
    return Scrip_DF

def CM_Bhav_Data (Scrip_DF):
    
    # print (Scrip_DF)
    # There is a empty column from source file
    Scrip_DF = Scrip_DF.dropna (axis=1, how='all')
    # print (Scrip_DF)

    Scrip_DF['TIMESTAMP'] = Scrip_DF['TIMESTAMP'].dt.date  # Convert to datetime    
    Scrip_DF['OPEN'].apply (lambda x : float(x))
    Scrip_DF['HIGH'].apply (lambda x : float(x))       # Ensure float
    Scrip_DF['LOW'].apply (lambda x : float(x))         # Ensure float
    Scrip_DF['CLOSE'].apply (lambda x : float(x))     # Ensure float
    Scrip_DF['LAST'].apply (lambda x : float(x))       # Ensure float
    Scrip_DF['PREVCLOSE'].apply (lambda x : float(x))     # Ensure float
    Scrip_DF['TOTTRDQTY'].apply (lambda x : int(x))   # Ensure integer
    Scrip_DF['TOTTRDVAL'].apply (lambda x : float(x))     # Ensure float
    Scrip_DF['TOTALTRADES'].apply (lambda x : int(x))   # Ensure integer
    # Scrip_DF.columns = Scrip_DF.columns.str.lower()
    
    return Scrip_DF

def New_CM_Bhav_Data (Scrip_DF):
    
    # Remove trailing space in the column titles
    Scrip_DF.columns = Scrip_DF.columns.str.strip ()
    # print (Scrip_DF)

    # Reorder the columns
    Scrip_DF = Scrip_DF.loc [:, ['SYMBOL', 'SERIES', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'LAST_PRICE', 'PREV_CLOSE', 'TTL_TRD_QNTY', 'TURNOVER_LACS',\
                                'DATE1', 'NO_OF_TRADES', 'AVG_PRICE', 'DELIV_QTY', 'DELIV_PER']]
    # print (Scrip_DF)

    Scrip_DF['DATE1'] = Scrip_DF['DATE1'].dt.date  # Convert to datetime    
    Scrip_DF['OPEN_PRICE'].apply (lambda x : float(x))
    Scrip_DF['HIGH_PRICE'].apply (lambda x : float(x))       # Ensure float
    Scrip_DF['LOW_PRICE'].apply (lambda x : float(x))         # Ensure float
    Scrip_DF['CLOSE_PRICE'].apply (lambda x : float(x))     # Ensure float
    Scrip_DF ['LAST_PRICE'] = pd.to_numeric (Scrip_DF ['LAST_PRICE'], errors='coerce')
    # Scrip_DF['LAST_PRICE'].apply (lambda x : float(x))       # Ensure float
    Scrip_DF['PREV_CLOSE'].apply (lambda x : float(x))     # Ensure float
    Scrip_DF['TTL_TRD_QNTY'].apply (lambda x : int(x))   # Ensure integer
    Scrip_DF['TURNOVER_LACS'] = (Scrip_DF['TURNOVER_LACS'].astype (float)) * 100000.0
    Scrip_DF['NO_OF_TRADES'].apply (lambda x : int(x))   # Ensure integer
    Scrip_DF['AVG_PRICE'].apply (lambda x : float(x))   # Ensure float
    Scrip_DF ['DELIV_QTY'] = pd.to_numeric (Scrip_DF ['DELIV_QTY'], errors='coerce')
    Scrip_DF ['DELIV_PER'] = pd.to_numeric (Scrip_DF ['DELIV_PER'], errors='coerce')
    Scrip_DF['DELIV_QTY'] = Scrip_DF['DELIV_QTY'].fillna (0).astype (int)
    # Scrip_DF['DELIV_PER'].apply (lambda x : float(x))   # Ensure float
    
    Scrip_DF.insert (len(Scrip_DF.columns) - 3, 'ISIN', None)
    # print (Scrip_DF)
    
    return Scrip_DF

def Read_FnO_Bhav (File_Path) :

    Data = pd.read_csv (File_Path, parse_dates=['TIMESTAMP', 'EXPIRY_DT'], date_format='%d-%b-%Y')
    Data = FnO_Bhav_Data (Data)

    return Data

def Read_New_FnO_Bhav (File_Path) :

    ## Clear the Tmp folder of old files
    shutil.rmtree('./Tmp', ignore_errors=True)

    # Extract the zip content
    with zipfile.ZipFile(File_Path, 'r') as zip_ref:
        zip_ref.extractall('.\\Tmp')    

    File = os.listdir('.\\Tmp')[0]

    Data = pd.read_csv ('.\\Tmp\\'+File, parse_dates=['TradDt', 'XpryDt'])
    # os.remove('.\\Tmp\\'+File)

    Data = New_FnO_Bhav_Data (Data)    

    return Data

def Read_CM_Bhav (File_Path) :

    Data = pd.read_csv (File_Path, parse_dates=['TIMESTAMP'])
    Data = CM_Bhav_Data (Data)

    return Data

def Read_New_CM_Bhav (File_Path) :

    Data = pd.read_csv (File_Path, parse_dates=[' DATE1'])
    Data = New_CM_Bhav_Data (Data)

    return Data

def Read_Participant_Data (File_Path, File_Name, type) :

    Data = pd.read_csv (File_Path, skiprows=1)
    # fao_participant_oi_04102024    

    if (type == 'vol'):
        Date = datetime.strptime (File_Name, 'fao_participant_vol_%d%m%Y.csv')
    else:
        Date = datetime.strptime (File_Name, 'fao_participant_oi_%d%m%Y.csv')

    Data = Data.dropna (axis=1, how='all')
    Data ['DATE'] = Date
    Data = Participant_Data (Data)
    # print (Data)

    return Data

def Read_Daily_Vol (File_Path, File_Name) :

    Data = pd.read_csv (File_Path, skiprows=4,header=None)
    Data.columns = ['REC_TYPE', 'SR_NO', 'SYMBOL', 'SERIES', 'QTY_TRD', 'DEL_QTY', 'DEL_%']
    Date = datetime.strptime (File_Name, 'MTO_%d%m%Y.DAT')
    Data ['TIMESTAMP'] = Date
    Data = Daily_Vol_Data (Data)
    # print (Data)

    return Data

# Define the insert query with ON CONFLICT clause to handle duplicates
cm_bhav_query = """
    INSERT INTO raw_data.eod_cm_bhav (symbol, series, open, high, low, close, last, prevclose, tottrdqty, tottrdval, timestamp, totaltrades, isin)
    VALUES %s
    ON CONFLICT (symbol, timestamp) DO NOTHING;
"""
new_cm_bhav_query = """
    INSERT INTO raw_data.eod_cm_bhav (symbol, series, open, high, low, close, last, prevclose, tottrdqty, tottrdval, timestamp, totaltrades, isin, avg_price, deliv_qty, deliv_per)
    VALUES %s
    ON CONFLICT (symbol, timestamp) DO NOTHING;
"""

daily_vol_query = """
    INSERT INTO raw_data.eod_daily_vol (\"record type\", \"sr no\", symbol, series, \"quantity traded\", \"deliverable quantity\", \"del percent\", timestamp)
    VALUES %s
    ON CONFLICT (symbol, timestamp) DO NOTHING;
"""

participant_oi_query = """
    INSERT INTO raw_data.eod_participant_oi (Client_Type,Future_Index_Long,Future_Index_Short,Future_Stock_Long,Future_Stock_Short,Option_Index_Call_Long,
    Option_Index_Put_Long,Option_Index_Call_Short,Option_Index_Put_Short,Option_Stock_Call_Long,Option_Stock_Put_Long,Option_Stock_Call_Short,Option_Stock_Put_Short,
    Total_Long_Contracts,Total_Short_Contracts,date)
    VALUES %s
    ON CONFLICT (Client_Type, date) DO NOTHING;
"""
participant_vol_query = """
    INSERT INTO raw_data.eod_participant_vol (Client_Type,Future_Index_Long,Future_Index_Short,Future_Stock_Long,Future_Stock_Short,Option_Index_Call_Long,
    Option_Index_Put_Long,Option_Index_Call_Short,Option_Index_Put_Short,Option_Stock_Call_Long,Option_Stock_Put_Long,Option_Stock_Call_Short,Option_Stock_Put_Short,
    Total_Long_Contracts,Total_Short_Contracts,date)
    VALUES %s
    ON CONFLICT (Client_Type, date) DO NOTHING;
"""

fno_bhav_query = """
    INSERT INTO raw_data.eod_fno_bhav (instrument,symbol,expiry_dt,open,high,low,close,settle_pr,contracts,val_inlakh,open_int,chg_in_oi,timestamp)
    VALUES %s
    ON CONFLICT (timestamp, symbol, expiry_dt) DO NOTHING;
"""

new_fno_bhav_query = """
    INSERT INTO raw_data.eod_fno_bhav (instrument,symbol,expiry_dt,open,high,low,close,settle_pr,contracts,val_inlakh,open_int,chg_in_oi,timestamp, underlying_pr)
    VALUES %s
    ON CONFLICT (timestamp, symbol, expiry_dt) DO NOTHING;
"""

Curr_Time = datetime.now ()
Log_File = 'Log_'+Curr_Time.strftime ('%d-%b-%Y_%H%M%S')+'.txt'

with open (Log_File, 'w') as File:
    File.write ('Data Loading into DB '+DATABASE+' Table '+'eod_cm_bhav'+'... @ '+Curr_Time.strftime ('%d-%b-%Y %H:%M:%S')+'\n')

for sub_fold in Sub_Folder_Names :

    Folder = Folder_Path + sub_fold

    ## Based on Data selected
    if (Category == 'CM_Bhav') :
        Files = list_bhav_files_in_folder (Folder)
    elif (Category == 'New_CM_Bhav') :
        Files = list_new_bhav_files_in_folder (Folder)
    elif (Category == 'Daily_Vol') :
        Files = list_dat_files_in_folder (Folder)
    elif (Category == 'Participant_OI') :
        Files = list_oi_files_in_folder (Folder)
    elif (Category == 'Participant_Vol') :
        Files = list_vol_files_in_folder (Folder)
    elif (Category == 'FnO_Bhav') :
        Files = list_bhav_files_in_folder (Folder)
    elif (Category == 'New_FnO_Bhav') :
        Files = list_new_fno_bhav_files_in_folder (Folder)

    for file in Files :

        File_Path = Folder+'\\'+file
        
        try :

            ## Based on Data selected
            if (Category == 'CM_Bhav') :
                Data = Read_CM_Bhav (File_Path)
                Query = cm_bhav_query

            elif (Category == 'New_CM_Bhav') :
                Data = Read_New_CM_Bhav (File_Path)
                Query = new_cm_bhav_query

            elif (Category == 'Daily_Vol') :
                Data = Read_Daily_Vol (File_Path, file)
                Query = daily_vol_query

            elif (Category == 'Participant_OI') :
                Data = Read_Participant_Data (File_Path, file, 'oi')
                Query = participant_oi_query

            elif (Category == 'Participant_Vol') :
                Data = Read_Participant_Data (File_Path, file, 'vol')
                Query = participant_vol_query

            elif (Category == 'FnO_Bhav') :
                Data = Read_FnO_Bhav (File_Path)
                Query = fno_bhav_query

            elif (Category == 'New_FnO_Bhav') :
                Data = Read_New_FnO_Bhav (File_Path)
                Query = new_fno_bhav_query

            data_tuples = list(Data.itertuples(index=False, name=None))
            execute_values(cur, Query, data_tuples)
            conn.commit()

            with open (Log_File, 'a') as File:
                File.write ('++ Data inserted from File : '+File_Path+'\n')             

            print ('++ Data processed from File : '+file)   

        except Exception:

            # print (str(Exception))
            traceback.print_exc()
            print (Data)
            with open (Log_File, 'a') as File:
                File.write ('!! Error Processing File : '+File_Path+'\n')


cur.close()
conn.close()
