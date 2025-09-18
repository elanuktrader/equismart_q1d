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
#reload(load_sql_functions)
#reload(..production.utility_py_functions)




if __name__ == "__main__":
    
    '''function_name = 'compute_fno_summary'
    parameters = (60, 'ABB')
    feature_sql = 'c:/Users/elan4/OneDrive/Documents/GitHub/equismart/sql_scripts/production/fno_sql_functions.sql' 
    '''
    '''function_name = 'compute_cm_summary'
    parameters = (60, 'ABB')
    feature_sql = 'c:/Users/elan4/OneDrive/Documents/GitHub/equismart/sql_scripts/production/cm_sql_functions.sql'
    load_sql_functions(feature_sql) '''

    print("----New Feature Addition start Timestamp:", pd.Timestamp.now())
    yaml_config_path = 'c:/Users/elan4/OneDrive/Documents/GitHub/equismart/config/feature_config/test_summary_generation.yaml'  # Path to your YAML config
    process_add_feature_yaml(yaml_config_path)
    print("----New Feature Addition Finish Timestamp:", pd.Timestamp.now())

    # 