import zipfile
from urllib import request
import pandas as pd
from psycopg2.extras import execute_values
import datetime

##############################################################################################################
################################ Extract Functions   ############################################################

def extract_stock_raw_df(api,Exch,Token,Start_Date,End_Date,Inter):
    ret = api.get_time_price_series(exchange=Exch, token=Token, starttime=Start_Date.timestamp(),endtime= End_Date.timestamp(),interval = Inter)
    raw_df = pd.DataFrame(ret)

    return raw_df

def extract_Expiry_SM(Symbol,NFO_IDX_FUT):
 
    Index_DF = NFO_IDX_FUT.loc[NFO_IDX_FUT['Symbol']==Symbol]
    
    Tsym_DF = Index_DF['TradingSymbol']
    print(Tsym_DF)
    F1_Expiry = (Tsym_DF.iloc[0])[-8:]
    F2_Expiry = (Tsym_DF.iloc[1])[-8:]
    F3_Expiry = (Tsym_DF.iloc[2])[-8:]
    
    print(Symbol, 'Verify the Expiry dates',F1_Expiry, F2_Expiry,F3_Expiry )
    
    return F1_Expiry,F2_Expiry,F3_Expiry

def extract_NFO_scrip_master_data():


    print('Fetching F&O Scrip Master from Finvasia')

    NFO_Name = 'NFO_symbols.txt.zip'
    NFO_File = 'NFO_symbols.txt'

    

    request.urlretrieve('https://api.shoonya.com/NFO_symbols.txt.zip', NFO_Name)
    with zipfile.ZipFile(NFO_Name, 'r') as zip_ref:
        zip_ref.extractall()


    NFO_Master_List = pd.read_csv(NFO_File)
    NFO_STK_FUT = NFO_Master_List.loc[NFO_Master_List['Instrument']=='FUTSTK']
    NFO_IDX_FUT = NFO_Master_List.loc[NFO_Master_List['Instrument']=='FUTIDX']
    NFO_IDX_FUT = NFO_IDX_FUT.copy()
    NFO_IDX_FUT['Expiry'] = NFO_IDX_FUT['Expiry'].str.strip()
    NFO_IDX_FUT['Expiry'] = pd.to_datetime(NFO_IDX_FUT['Expiry'], format='%d-%b-%Y', errors='raise')

    NFO_IDX_FUT = NFO_IDX_FUT.sort_values(by='Expiry')

    return NFO_STK_FUT, NFO_IDX_FUT 


def extract_NSE_scrip_master_data():

    print('Fetching Cash market (NSE) Scrip Master from Finvasia')
    NSE_Name = 'NSE_symbols.txt.zip'
    NSE_File = 'NSE_symbols.txt'
    request.urlretrieve('https://api.shoonya.com/NSE_symbols.txt.zip', NSE_File)
    with zipfile.ZipFile(NSE_Name, 'r') as zip_ref:
        zip_ref.extractall()


    NFO_Master_List = pd.read_csv(NSE_File)
    NFO_STK_FUT = NFO_Master_List.loc[NFO_Master_List['Instrument']=='FUTSTK']
    NFO_IDX_FUT = NFO_Master_List.loc[NFO_Master_List['Instrument']=='FUTIDX']
    NFO_IDX_FUT = NFO_IDX_FUT.copy()
    NFO_IDX_FUT['Expiry'] = NFO_IDX_FUT['Expiry'].str.strip()
    NFO_IDX_FUT['Expiry'] = pd.to_datetime(NFO_IDX_FUT['Expiry'], format='%d-%b-%Y', errors='raise')

    NFO_IDX_FUT = NFO_IDX_FUT.sort_values(by='Expiry')

    return NFO_STK_FUT, NFO_IDX_FUT 


def extract_fno_scripdetails(Scrip_name,NSE_Name,NFO_STK_FUT,NFO_IDX_FUT,Exchange='NFO'):
    Symbol_Token = 0
    Symbol_Exch = Exchange  
    Specif_scrip_len = 0

    if(NSE_Name=='BANKNIFTY' or NSE_Name=='NIFTY' or NSE_Name=='FINNIFTY' ):
        
        Specif_scrip = NFO_IDX_FUT.loc[NFO_IDX_FUT['TradingSymbol']==Scrip_name]
                     
                
    else:                
        Specif_scrip = NFO_STK_FUT.loc[NFO_STK_FUT['TradingSymbol']==Scrip_name]
            
    Specif_scrip_len = len(Specif_scrip.index)
    
    if(Specif_scrip_len>=1):
        
        Specif_scrip_list = Specif_scrip.values.tolist()
        #print('Specif_scrip_list',Specif_scrip_list)
        Symbol_Token =   str(Specif_scrip_list[0][1])
        Symbol_Exch = 'NFO'
        print("Fetching token for: ",Scrip_name,' from Scrip Master. Token is',Symbol_Token)

    else:
        Symbol_Token = 0
        Symbol_Exch = 'NFO'             
        
       
            
        
    return Symbol_Token,Symbol_Exch


##############################################################################################################
################################ Transform Functions   ############################################################


def Merge_Future_Data(Stocks_DF,Future_DF1,Future_DF2,Future_DF3):
    
    Future_DF1.drop(['Date','Time'], axis = 1,inplace=True)
    Future_DF2.drop(['Date','Time'], axis = 1,inplace=True)
    #print(Future_DF3)
    Future_DF3.drop(['Date','Time'], axis = 1,inplace=True)
    
    Stocks_DF = Stocks_DF.merge(Future_DF1,on ='DT',how ='left')
    Stocks_DF = Stocks_DF.merge(Future_DF2,on ='DT',how ='left')
    Stocks_DF = Stocks_DF.merge(Future_DF3,on ='DT',how ='left')
    
    return Stocks_DF





def transform_CSV_Stock_DF(Exch,raw_df,Series):
    
   
    
    raw_df['date'] = pd.to_datetime(raw_df['time'], format='%d-%m-%Y %H:%M:%S')
    Stocks_DF = raw_df.sort_values(by=['date'], ascending=True)
    
    Stocks_DF['Dates'] = Stocks_DF['date']. dt. date
    Stocks_DF['Times'] = Stocks_DF['date']. dt. time
    date_column = Stocks_DF.pop('date')
    Date_column = Stocks_DF.pop('Dates')
    Time_Column = Stocks_DF.pop('Times')
    Stocks_DF.insert(1, 'DT', date_column)
    Stocks_DF.insert(2, 'Date', Date_column)
    Stocks_DF.insert(3, 'Time', Time_Column)
    

    
    Stocks_DF.drop(['stat','time','ssboe'], axis = 1,inplace=True)
    if(Exch == 'NSE'):
        Stocks_DF.drop(['intoi','oi'], axis = 1,inplace=True)
    else:
        C_COI = Series+'COI'
        C_OI = Series+"OI"
        Stocks_DF.rename(columns = {'intoi':C_COI, 'oi':C_OI}, inplace = True)        
        Stocks_DF[C_COI] = Stocks_DF[C_COI].astype(int)
        Stocks_DF[C_OI] = Stocks_DF[C_OI].astype(int)
        
    
        
    C_O = Series+'Open'
    C_H = Series+'High'
    C_L = Series+'Low'
    C_C = Series+'Close'
    C_IV = Series+'Volume'
    C_CV = Series+'Cum_Vol'
    C_VWAP = Series+'VWAP'
        
    Stocks_DF.rename(columns = {'into':C_O, 'inth':C_H,'intl':C_L, 'intc':C_C, 'intv':C_IV,'v':C_CV, 'intvwap':C_VWAP}, inplace = True)
    
    Stocks_DF[C_O] = Stocks_DF[C_O].astype(float)
    Stocks_DF[C_H] = Stocks_DF[C_H].astype(float)
    Stocks_DF[C_L] = Stocks_DF[C_L].astype(float)
    Stocks_DF[C_C] = Stocks_DF[C_C].astype(float)
    Stocks_DF[C_IV] = Stocks_DF[C_IV].astype(int)
    
 

    return Stocks_DF

def transform_SQL_Stock_DF(Exch,raw_df,NSE_Name,Series=''):
    
    Series = Series[:2]
    
    raw_df['timestamp'] = pd.to_datetime(raw_df['time'], format='%d-%m-%Y %H:%M:%S')
    raw_df.drop(['stat','time','ssboe'], axis = 1,inplace=True)
    Stocks_DF = raw_df.sort_values(by=['timestamp'], ascending=True)
    Stocks_DF['nse_symbol'] = NSE_Name
    Stocks_DF['Time'] = Stocks_DF['timestamp']. dt. time  
    
    if(Exch == 'NSE'):
        Stocks_DF = Stocks_DF.loc[(Stocks_DF['Time']>=datetime.time(9,15)) & (Stocks_DF['Time']<=datetime.time(15,29))]
        Stocks_DF.drop(['intoi','oi'], axis = 1,inplace=True)
    else:
        Stocks_DF = Stocks_DF.loc[(Stocks_DF['Time']>datetime.time(9,15)) & (Stocks_DF['Time']<=datetime.time(15,29))]
        Stocks_DF.rename(columns = {'intoi':'coi'}, inplace = True)        
        Stocks_DF['coi'] = Stocks_DF['coi'].fillna(0).astype('int32')
        Stocks_DF['oi'] = Stocks_DF['oi'].astype('int32')
        Stocks_DF['fut_series'] = Series
        Stocks_DF['fut_series'] = Stocks_DF['fut_series'].astype('string')

        
    
        
    C_O = 'open'
    C_H = 'high'
    C_L = 'low'
    C_C = 'close'
    C_IV = 'volume'
    C_CV = 'cum_vol'
    C_VWAP = 'vwap'
        
    Stocks_DF.rename(columns = {'into':C_O, 'inth':C_H,'intl':C_L, 'intc':C_C, 'intv':C_IV,'v':C_CV, 'intvwap':C_VWAP}, inplace = True)
    
    Stocks_DF[C_O] = Stocks_DF[C_O].astype(float)
    Stocks_DF[C_H] = Stocks_DF[C_H].astype(float)
    Stocks_DF[C_L] = Stocks_DF[C_L].astype(float)
    Stocks_DF[C_C] = Stocks_DF[C_C].astype(float)
    Stocks_DF[C_IV] = Stocks_DF[C_IV].astype('int32')
    Stocks_DF[C_CV] = Stocks_DF[C_CV].astype('int32')
    Stocks_DF[C_VWAP] = Stocks_DF[C_VWAP].astype(float)

    if(Exch == 'NFO'):
        Stocks_DF = Stocks_DF[['nse_symbol', 'timestamp', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'cum_vol', 'coi','oi','fut_series']]
        valid_df = Stocks_DF.loc[
                    (Stocks_DF["open"] > 0) &
                    (Stocks_DF["high"] > 0) &
                    (Stocks_DF["low"] > 0) &
                    (Stocks_DF["close"] > 0) &
                    (Stocks_DF["volume"] >= 0) &
                    (Stocks_DF["vwap"] >= 0) &
                    (Stocks_DF["cum_vol"] >= 0) &
                    (Stocks_DF["oi"] >= 0)
                ]
    else:
        Stocks_DF = Stocks_DF[['nse_symbol', 'timestamp', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'cum_vol']]
        valid_df = Stocks_DF.loc[
                     (Stocks_DF["open"] > 0) &
                     (Stocks_DF["high"] > 0) &
                     (Stocks_DF["low"] > 0) &
                     (Stocks_DF["close"] > 0) &
                     (Stocks_DF["vwap"] >= 0) &
                     (Stocks_DF["volume"] >= 0) &
                     (Stocks_DF["cum_vol"] >= 0)
                 ]

    
 

    return valid_df


##############################################################################################################
################################ Load Functions   ############################################################





def load_cm_data(cm_stock_df,NSE_Name,conn,insert_query):
    cur = conn.cursor()
    try:
        #cm_stock_df.to_csv('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/test_output/'+NSE_Name+'.csv')
        data_tuples = list(cm_stock_df.itertuples(index=False, name=None))
        execute_values(cur, insert_query, data_tuples)
        conn.commit()
        print('Total number of Cash market Data rows inserted into the database for',NSE_Name,'is',len(cm_stock_df))
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {e}")

def load_fno_data(sql_F1_DF,sql_F2_DF,sql_F3_DF,NSE_Name,conn,insert_query):
    cur = conn.cursor()
    sql_fno_df = pd.concat([sql_F1_DF, sql_F2_DF, sql_F3_DF], axis=0, ignore_index=True)

    
    try:
        #sql_fno_df.to_csv('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/test_output/'+NSE_Name+'_fno.csv')
        data_tuples = list(sql_fno_df.itertuples(index=False, name=None))
        execute_values(cur, insert_query, data_tuples)
        conn.commit()
        print('Total number of fno data rows inserted into the database for',NSE_Name,'is',len(sql_fno_df))
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {e}")

def load_CSV_Stock_DF(Stocks_DF,Operation,Dest_File):
    
    if(Operation == 'Create'):
        print('Creating new file:',Dest_File)
        
        Stocks_DF.to_csv(Dest_File, mode='w', index=True, header=True)
    elif(Operation == 'Append'):
        data = pd.read_csv(Dest_File,usecols=range(5))
        data['DT'] = pd.to_datetime(data['DT'], format='mixed', errors='coerce')
        latest_date = data['DT'].dt.date.max()
        print('Rows before latest data check', len(Stocks_DF))
        print(f"Latest date available for {Dest_File} is {latest_date}")
        Stocks_DF_filt = Stocks_DF.loc[Stocks_DF['Date']>latest_date]
        print('Rows to be appended',len(Stocks_DF_filt))
        Stocks_DF_filt.to_csv(Dest_File, mode='a', index=True, header=False)
        if(len(Stocks_DF_filt)!=0):
            print('Appending the CSV file:',Dest_File)
        
           # Stocks_DF_filt.to_csv(Dest_File, mode='a', index=True, header=False)
        else:
            print("No Data appended as aleady the latest data is available in the file:",Dest_File)

def generate_data_unavail_report(Data_not_available_list,Start_Index_1min,Stop_Index_1min):
    Data_unavailable_df = pd.DataFrame(Data_not_available_list)
    Data_unavail_report_path = './report/data_not_available.csv'
    # Data_unavailable_df.to_csv(Data_unavail_report_path)
    print('No of stocks with no latest data', len(Data_unavailable_df))
    if(len(Data_unavailable_df)!=0):
        if(Start_Index_1min==0 and Stop_Index_1min ==0):
            Data_unavailable_df.to_csv(Data_unavail_report_path)
        else:
            Data_unavailable_df.to_csv(Data_unavail_report_path, mode='a', index=True, header=False)  
        print('Check the stock data unavailability report in the location:',Data_unavail_report_path)
    else:
        print('Data availability health is fine')


def read_default_stock_list(csv_file):
    df = pd.read_csv(csv_file)  
    #print(df)
    df = df.loc[df['Sum_Gen']=='Yes']
    
    df = df.dropna().reset_index(drop=True)
    print('number of stocks for feature computation is', len(df))
    #print(df)
    stock_list = df.iloc[:, 0].tolist()
    #print(stock_list)
    #print(','.join(stock_list))
    stock_list_csl = ",".join(stock_list)
    return stock_list_csl

# Back_up Code:

# def extract_stock_raw_df(Exch,Token,Start_Date,End_Date,Inter):
#     ret = api.get_time_price_series(exchange=Exch, token=Token, starttime=Start_Date.timestamp(),endtime= End_Date.timestamp(),interval = Inter)
#     raw_df = pd.DataFrame(ret)

#     return raw_df

# def extract_fno_scripdetails(Scrip_name,NSE_Name,Exchange='NFO'):
#     global NFO_STK_FUT
#     global NFO_IDX_FUT
#     Symbol_Token = 0
#     Symbol_Exch = Exchange  
#     Specif_scrip_len = 0

#     if(NSE_Name=='BANKNIFTY' or NSE_Name=='NIFTY' or NSE_Name=='FINNIFTY' ):
        
#         Specif_scrip = NFO_IDX_FUT.loc[NFO_IDX_FUT['TradingSymbol']==Scrip_name]
                     
                
#     else:                
#         Specif_scrip = NFO_STK_FUT.loc[NFO_STK_FUT['TradingSymbol']==Scrip_name]
            
#     Specif_scrip_len = len(Specif_scrip.index)
    
#     if(Specif_scrip_len>=1):
        
#         Specif_scrip_list = Specif_scrip.values.tolist()
#         #print('Specif_scrip_list',Specif_scrip_list)
#         Symbol_Token =   str(Specif_scrip_list[0][1])
#         Symbol_Exch = 'NFO'
#         print("Fetching token for: ",Scrip_name,' from Scrip Master. Token is',Symbol_Token)
#     elif(Specif_scrip_len==0):
#         NSE_Name = Scrip_name[:-8]
#         F1_Scrip = NSE_Name+F1_Expiry
#         print(F1_Scrip)
#         API_Sym = api.searchscrip('NFO', F1_Scrip)
#         print('In Future Location: API Sym Class (NOT GOOD SIGN',API_Sym.__class__())
#         if(API_Sym.__class__() != None):
        
#             Symbol_list = pd.DataFrame(API_Sym['values']).values.tolist()
#             print('In Future Location:',print(len(Symbol_list)))
#             Symbol_Token = Symbol_list[0][1]
#             Symbol_Exch = Symbol_list[0][0]
#         else:
#             Symbol_Token = 0
#             Symbol_Exch = 'NFO'

#     else:
#         Symbol_Token = 0
#         Symbol_Exch = 'NFO'             
        
       
            
        
#     return Symbol_Token,Symbol_Exch