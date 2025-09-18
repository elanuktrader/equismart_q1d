# -*- coding: utf-8 -*-
"""
Created on Mon Dec 30 05:46:44 2024

@author: elan4
"""

import os
import pandas as pd



# List to store stock names and latest dates
latest_date_list = []
Stock_list_file_path = 'c:/Users/elan4/OneDrive/Documents/GitHub/equismart/config/maintenance_config/Stock_list_No_latest_data.csv'
Stocks_list_DF = pd.read_csv(Stock_list_file_path)
Stocks_List = Stocks_list_DF.values.tolist()
print('=====================================================================')
Start_Index= 0
Stop_Index = 0




Raw_Loc = "E:\\Trading\\Finvasia_API\\Daily\\Raw_Data\\Min_1\\"
print(len(Stocks_List))
if(Stop_Index==0):
    Stop_Index = len(Stocks_List)

if(Start_Index<=Stop_Index):
    for i in range(Stop_Index):
        
        
        if (i>=Start_Index and i < Stop_Index):
            Symbol = Stocks_List[i][1]
            NSE_Name = Stocks_List[i][0]
            Fut_Stock = Stocks_List[i][2]
            Symbol_Token = str(int(Stocks_List[i][5]))
            Dest_File = Raw_Loc+Symbol+'.csv'
            #try:
                # Load CSV and get the latest date
            data = pd.read_csv(Dest_File,usecols=range(3))
            data['DT'] = pd.to_datetime(data['DT'], format='mixed', errors='coerce')
            latest_date = data['DT'].dt.date.max()
            print('Current Index:',i,NSE_Name,latest_date)
            #latest_date = pd.to_datetime(data['date'])  # Ensure 'date' is a valid datetime column
            latest_date_list.append({"Name": NSE_Name, "Latest Date": latest_date})
            #except Exception as e:
            #    print(f"Error processing {Dest_File}: {e}")
            
            
    
    # Convert the list to a DataFrame
summary_df = pd.DataFrame(latest_date_list)

# Save the DataFrame to a new CSV
output_csv_path = "c:/Users/elan4/OneDrive/Documents/GitHub/equismart/report/latest_dates_summary.csv"
summary_df.to_csv(output_csv_path, index=False)
print(f"Summary CSV saved to {output_csv_path}")