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
from ..production.utility_py_functions import load_sql_functions
import csv
#from importlib import reload
#reload(load_sql_functions)





def run_main_sql(script_path):

    output_csv = 'c:/Users/elan4/OneDrive/Documents/GitHub/equismart/test_output/test_fno_results_ABB.csv'
    conn = get_processor_db_connection()
    cur = conn.cursor()

    with open(script_path, 'r') as sql_file:
        sql_script = sql_file.read()
    
    print("Executing test SQL script...")
    cur.execute(sql_script)

    # Fetch results for validation
    results = cur.fetchall()

    # Get column names
    col_names = [desc[0] for desc in cur.description]

    # Write results to CSV
    with open(output_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # Write the header row
        writer.writerow(col_names)
        print(col_names)
        
        # Write data rows
        writer.writerows(results)

    print(f"Results written to {output_csv}")
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_sql_functions('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/sql_scripts/production/fno_sql_functions_derived.sql')
    print("----computation start Timestamp:", pd.Timestamp.now())
    run_main_sql('c:/Users/elan4/OneDrive/Documents/GitHub/equismart/sql_scripts/testing/fno_data_processing_tester.sql')
    print("----computation Finish Timestamp:", pd.Timestamp.now())

    # 