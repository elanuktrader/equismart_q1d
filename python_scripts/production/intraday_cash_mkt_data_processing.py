import psycopg2
from psycopg2.extras import execute_values
import sys
sys.path.append('c:\\Users\\elan4\\OneDrive\\Documents\\GitHub\\equismart\\config')
sys.path.append('c:\\Users\\elan4\\OneDrive\\Documents\\GitHub\\equismart')


# PostgreSQL connection parameters
DATABASE = 'NSE_Stock_Data'
USER = 'data_processor'
PASSWORD = 'processor'
HOST = 'localhost'
PORT = '5432'

# Set up the database connection
conn = psycopg2.connect(
    dbname=DATABASE,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
)



def run_main_sql(script_path):
    conn = get_db_connection()
    cur = conn.cursor()

    with open(script_path, 'r') as sql_file:
        sql_script = sql_file.read()
    
    print("Executing Main SQL script...")
    cur.execute(sql_script)
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_main_sql('./sql_scripts/Volume_Data_Processing.sql')