import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import string_types
import sys
import yaml
sys.path.append('c:\\Users\\elan4\\OneDrive\\Documents\\GitHub\\equismart\\config')
from config.db_config import get_processor_db_connection

type_mapping = {
    23: 'INTEGER',        # OID 23 maps to INTEGER
    20: 'BIGINT',         # OID 20 maps to BIGINT
    21: 'SMALLINT',       # OID 21 maps to SMALLINT
    701: 'FLOAT',         # OID 701 maps to FLOAT (DOUBLE PRECISION)
    1700: 'NUMERIC',      # OID 1700 maps to NUMERIC
    1043: 'VARCHAR(50)',  # OID 1043 maps to VARCHAR
    25: 'TEXT',           # OID 25 maps to TEXT
    18: 'CHAR(1)',        # OID 18 maps to CHAR
    1082: 'DATE',         # OID 1082 maps to DATE
    1083: 'TIME',         # OID 1083 maps to TIME
    1114: 'TIMESTAMP',    # OID 1114 maps to TIMESTAMP
    1184: 'TIMESTAMPTZ',  # OID 1184 maps to TIMESTAMPTZ
    16: 'BOOLEAN',        # OID 16 maps to BOOLEAN
    17: 'BYTEA',          # OID 17 maps to BYTEA
    2950: 'UUID',         # OID 2950 maps to UUID
    114: 'JSON',          # OID 114 maps to JSON
    3802: 'JSONB',        # OID 3802 maps to JSONB
    600: 'POINT',         # OID 600 maps to POINT
    628: 'LINE',          # OID 628 maps to LINE
    601: 'LSEG',          # OID 601 maps to LSEG
    603: 'BOX',           # OID 603 maps to BOX
    602: 'PATH',          # OID 602 maps to PATH
    604: 'POLYGON',       # OID 604 maps to POLYGON
    718: 'CIRCLE',        # OID 718 maps to CIRCLE
    700: 'REAL',          # OID 700 maps to REAL (4-byte floating point)
}

def process_summary_yaml(config_path):
    conn = get_processor_db_connection()
    print('Config File path is',config_path)
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    for feature in config['features']:
        print(f"Processing feature: {feature['function_name']}")
        function_name = feature['function_name']
        parameters = feature['parameters']
        table_name = feature['table_name']
        sql_script_path = feature['sql_script_path']
        

        # Load SQL functions
        load_sql_functions(sql_script_path)

        # Get metadata and add columns to the table
        metadata = get_column_metadata(function_name, parameters)
        insert_query=get_insert_data_query(table_name, function_name, parameters, metadata)
        
        query_execution(insert_query, conn)

    conn.close()

def execute_summary_yaml(updated_yaml_data):
    conn = get_processor_db_connection()
    #print('Updated YAML Data is',updated_yaml_data )

    for feature in updated_yaml_data.get('features', []):
        function_name = feature.get('function_name')
        print("Excuting the function:",function_name)
        parameters = feature.get('parameters')
        table_name = feature.get('table_name')
        sql_script_path = feature.get('sql_script_path')
        # Load SQL functions
        load_sql_functions(sql_script_path)

        # Get metadata and add columns to the table
        metadata = get_column_metadata(function_name, parameters)
        insert_query=get_insert_data_query(table_name, function_name, parameters, metadata)
        
        query_execution(insert_query, conn)

    '''for feature in config['features']:
        print(f"Processing feature: {feature['function_name']}")
        function_name = feature['function_name']
        parameters = feature['parameters']
        table_name = feature['table_name']
        sql_script_path = feature['sql_script_path']'''
        

        

    conn.close()

def process_add_feature_yaml(config_path):
    conn = get_processor_db_connection()
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    for feature in config['features']:
        print(f"Processing feature: {feature['function_name']}")
        function_name = feature['function_name']
        parameters = feature['parameters']
        table_name = feature['table_name']
        sql_script_path = feature['sql_script_path']
        

        # Load SQL functions
        load_sql_functions(sql_script_path)

        # Get metadata and add columns to the table
        metadata = get_column_metadata(function_name, parameters)
        add_columns_to_table(table_name, metadata,conn)

    conn.close()

def query_execution(query, connection):

    """
        Execute a generic SQL query using an active PostgreSQL connection.

        Parameters:
            query (str): The SQL query to execute.
            connection (psycopg2.connection): Active PostgreSQL database connection.

        Returns:
            None
        """
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()
            print("Query executed successfully.")
    except Exception as e:
        connection.rollback()
        print(f"Error executing query: {e}")

def get_insert_data_query(table_name, function_name, parameters, metadata):

    """
    Generate an UPSERT SQL query for inserting data into a PostgreSQL table using the result of a function.

    Parameters:
        table_name (str): Name of the target table.
        function_name (str): Name of the function generating the data to insert.
        parameters (str): Comma-separated string of parameters to pass to the function.
        metadata (list): List of column names for the target table (excluding primary key columns).

    Returns:
        str: Generated UPSERT SQL query.
    """
    # Ensure metadata is not empty
    if not metadata:
        raise ValueError("Metadata cannot be empty. Provide at least one column name.")
    
     # Extract column names from metadata
    column_names = [col[0] for col in metadata]

    # First two columns assumed to be the conflict keys
    primary_Key_columns = ', '.join(column_names[:2])

    # Columns for INSERT and SET (excluding the first two columns)
    insert_columns = ', '.join(column_names)
    set_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in column_names[2:]])

    # Convert parameters list to comma-separated string
    #params_str = ', '.join([f"'{param}'" if isinstance(param, str) else str(param) for param in parameters])

    # Generate the UPSERT query
    query = f"""
    INSERT INTO {table_name} ({insert_columns})
    SELECT *
    FROM {function_name}{parameters }
    ON CONFLICT ({primary_Key_columns})
    DO UPDATE
    SET {set_columns};
    """.strip()

    return query

def add_columns_to_table(table_name, metadata,conn):
    """
    Add columns to a table based on metadata.

    Args:
        table_name (str): Name of the table to modify.
        metadata (list): List of tuples containing column names and their types.
    """
    try:
        #conn = get_processor_db_connection()
        cur = conn.cursor()

        with conn.cursor() as cur:
            for col_name, col_type in metadata:
                try:
                    print(col_name)
                    alter_query = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_name} {col_type};"
                    cur.execute(alter_query)
                except Exception as e:
                     print(f"Error adding columns to table: {e}{col_name}{col_type}")

        conn.commit()
        print(f"Columns added to table '{table_name}' successfully.")
    except Exception as e:
        print(f"Error adding columns to table: {e}")
    

def map_type_code_to_human_readable(type_code):
    """Map PostgreSQL type code to human-readable format."""
    return string_types.get(type_code, "Unknown Type")

def get_column_metadata(function_name, parameters):
    global type_mapping
    """Fetch column names and types returned by an SQL function."""
    

    conn = get_processor_db_connection()
    cur = conn.cursor()
    query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{function_name}'
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {function_name}{parameters};")
        desc = cur.description
        return [(col.name, type_mapping.get(col.type_code,col.type_code)) for col in desc]

def load_sql_functions(file_path):
    print("File path requested is",file_path)
    """Loads the SQL functions from a file into the PostgreSQL database."""
    
    try:
        conn = get_processor_db_connection()
        cur = conn.cursor()

        with open(file_path, 'r') as sql_file:
            print('SQL file name is',sql_file)
            sql_script = sql_file.read()
        
        print("Loading SQL functions...")
        cur.execute(sql_script)
        conn.commit()
        print('----------------------------------------------------')
        print("SQL functions loaded successfully.")
        

        schema_name = "public"  # Change this if your function is in a different schema

        check_query = f"""
            SELECT proname
            FROM pg_proc
            JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
            WHERE nspname = '{schema_name}';
        """

        cur.execute(check_query)
        function_info = cur.fetchall()
        print('function_info:',function_info)
        print('----------------------------------------------------')
        

    except Exception as e:
        print(f"Error loading SQL functions: {e}")



    finally:
        if cur:
            cur.close()  # Ensure cursor is closed
        if conn:
            conn.close()  # Ensure connection is closed


def execute_sql_function(function_name, params=None):
    """Executes a specific SQL function with optional parameters."""
    conn = get_processor_db_connection()
    cur = conn.cursor()

    if params:
        # Prepare the SQL query for the function call with parameters
        sql_query = f"SELECT {function_name}({', '.join(map(str, params))});"
    else:
        # Prepare the SQL query for the function call without parameters
        sql_query = f"SELECT {function_name}();"

    print(f"Executing SQL function: {function_name}")
    cur.execute(sql_query)
    
    # Fetch the result of the function execution
    results = cur.fetchall()
    
    # Commit any changes, if applicable
    conn.commit()
    
    cur.close()
    conn.close()
    
    return results


