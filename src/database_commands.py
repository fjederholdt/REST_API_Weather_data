import psycopg2
from psycopg2 import sql

def connect_to_database(host="localhost", database="weather_db", user="postgres", password="jabbu", port=5432):
    """
    Connect to a PostgreSQL database.
    
    Args:
        host (str): Database host address
        database (str): Database name
        user (str): Username
        password (str): Password
        port (int): Port number (default: 5432)
    
    Returns:
        connection: psycopg2 connection object
    """
    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def close_connection(connection):
    """Close database connection."""
    if connection:
        connection.close()

def execute_single_query(con,sql_insert_query):
    """Generic SQL execute
    """
    if con is not None:
        try:
            cursor = get_cursor(con)
            if len(sql_insert_query) == 2:
                cursor.execute(sql_insert_query[0],sql_insert_query[1])
            else:
                cursor.execute(sql_insert_query)
        except Exception as e:
            print(f"Error executing sql query: {sql_insert_query}, error: {e}")
    else:
        print("No connection to database")

def execute_multi_query(con,insert_sql):
    """Execute a list of SQL insert querys
    """
    if con is not None:
        try:
            for (insert,data) in insert_sql:
                execute_single_query(con,(insert,data))
        except Exception as e:
            print(f"Error getting table data: {e}")
            return 0
    else:
        print("No connection to database")
        return 0

def all_columns(con):
    """Gives the dict tables with their columns
    """
    c = get_cursor(con)
    c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = [r[0] for r in c.fetchall()]
    result = {}
    for tbl in tables:
        c.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{tbl}'")
        cols = [row[0] for row in c.fetchall()]
        result[tbl] = cols
    return result

def get_cursor(con):
    """Gets the cursor of the con
    """
    if con is not None:
        return con.cursor()
    else:
        print("No connection to database.")
        return None

def get_table(con,table):
    """Gets the data from a specific table\n
    Returns all data as a list
    """
    data = []
    cols = get_columns_from_table(con,table)
    if con is not None:
        try:
            cursor = get_cursor(con)
            cursor.execute(f"SELECT * FROM \"{table}\"")
            temp_data = cursor.fetchall()
            for entry in temp_data:
                temp_dict = {}
                for i,col in enumerate(cols):
                    temp_dict[col] = entry[i]
                data.append(temp_dict)
        except Exception as e:
            print(f"Error getting table data: {e}")
            return 0
    else:
        print("No connection to database")
        return 0
    return data

def get_column(con,table,column):
    """Gets data from a specific table, from a specific column
    Returns all data concerning the specific column
    """
    if con is not None:
        try:
            data = []
            cursor = get_cursor(con)
            cursor.execute(f"SELECT {column} FROM \"{table}\"")
            temp_data = cursor.fetchall()
            for d in temp_data:
                data.append(d[0])
        except Exception as e:
            print(f"Error getting column data: {e}")
            return 0
    else:
        print("No connection to database")
        return 0
    return data

def get_id(con,table,column,id):
    """Gets data from specific table, from specific column, with specific id
    Returns all data concerning the specific id
    """
    if con is not None:
        try:
            data = []
            cols = get_columns_from_table(con,table)
            cursor = get_cursor(con)
            cursor.execute(f"SELECT * FROM \"{table}\" WHERE \"{column}\" = '{id}'")
            temp_data = cursor.fetchall()
            for entry in temp_data:
                temp_dict = {}
                for i,col in enumerate(cols):
                    temp_dict[col] = entry[i]
                data.append(temp_dict)
        except Exception as e:
            print(f"Error getting id data: {e}")
            return 0
    else:
        print("No connection to database")
        return 0
    return data


def get_all_data(con):
    """Gets entire dataset from database
    """
    if con is not None:
        try:
            total = {}      
            cursor = get_cursor(con)
            schema = all_columns(con)
            for table, column in schema.items():
                for item in column:
                    cursor.execute(f"SELECT \"{item}\" FROM \"{table}\"")
                    data = cursor.fetchall()
                    cur_data = []
                    for i in data:
                        cur_data.append(i[0])
                    total[item] = cur_data
        except Exception as e:
            print(f"Error getting all data: {e}")
            return 0
    else:
        print("No connection to database")
        return 0
    return total

def get_columns_from_table(con,table):
    """Gets all columns from a table\n
    Returns as a list of column names
    """
    if con is not None:
        try:
            schema = all_columns(con)
            cols = schema[table]
            return cols
        except Exception as e:
            print(f"Error getting columns from table: {table}, error: {e}")
    else:
        print("No connection to database")

def get_tables_and_columns(schema):
    """Gets tables and columns from schema
    Returns a list of tables and a dict of columns
    """
    if schema is not None:
        try:
            tables = []
            columns = {}
            for table, column in schema.items():
                tables.append(table)
                columns[table] = column
        except Exception as e:
            print(f"Error extracting tables and columns: {e}")
            return 0
    else:
        print("No Schema given")
        return 0
    return tables, columns

def get_column_data_type(con, table, column):
    """Gets the specific datatype of a column in a table
    """
    if con is not None:
        try:
            c = get_cursor(con)
            c.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name={table!r}")
            cols = c.fetchall()
            for col in cols:
                if col[0] == column:
                    return col[1]
        except Exception as e:
            print(f"Error getting column datatype: {e}")
    else:
        print("No connection to database")
    
def get_datatypes(con,table,columns): 
    """Get datatypes for all columns in a table
        Returns dict of columns with their datatype
    """
    if con is not None:
        try:
            datatypes = {}
            for column in columns:
                datatypes[column] = get_column_data_type(con,table,column)
        except Exception as e:
            print(f"Error getting datatypes: {e}")
            return 0
    else:
        print("No connection to database")
        return 0
    return datatypes

def get_primary_key(con, table):
    """Gets the primary key column (name) of a table
    """
    if con is not None:
        try:
            cursor = get_cursor(con)
            cursor.execute(f"SELECT a.attname FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) WHERE c.conrelid='public.\"{table}\"'::regclass AND c.contype = 'p'")
            for row in cursor.fetchall():
                return row[0]
        except Exception as e:
            print(f"Error getting primary key: {e}")
            return 0
    else:
        print("No connection to database")
        return 0
    return 0
 
def get_connected_tables(con, parent_table): # Find tables that have a foreign key referencing the parent_table
    if con is not None:
        try:
            schema = all_columns(con)
            reference = []
            for table, columns in schema.items():
                cursor = get_cursor(con)
                cursor.execute(f"PRAGMA foreign_key_list({table})")
                for row in cursor.fetchall():
                    if row[2] == parent_table:
                        reference.append((table, row[3], row[4])) # row[3] is the column in the child table that references the parent table, row[4] is the column in the parent table that is referenced
        except Exception as e:
            print(f"Error getting connected tables: {e}")
            return 0
    else:
        print("No connection to database")
        return 0
    return reference
 
def get_mathing_ids(con, table, col, val):
    if con is not None:
        try:
            cursor = get_cursor(con)
            pk_column = get_primary_key(con, table)
            cursor.execute(f"SELECT {pk_column} FROM {table} WHERE {col} = ?", (val,))
            ids = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error getting matching ids: {e}")
            return 0
    else:
        print("No connection to database")
        return 0
    return ids, pk_column

def get_uniques(data: list):
    uniques = []
    for entry in data:
        if entry not in uniques:
            uniques.append(entry)
    return uniques

def get_measurement_from_station(con, station, measurement_type, timespan):
    """Gets measurements from a specific station within a timespan
    
    Args:
        con: Database connection
        station (str): Station name/id
        measurement_type (str): Type of measurement (e.g., 'temperature', 'humidity')
        timespan (dict): Dict with 'from' and 'to' datetime values
    
    Returns:
        list: Matching measurement entries
    """
    if con is not None:
        try:
            data = []
            cursor = get_cursor(con)
            measurement_column = ""
            if station == "DMI":
                    measurement_column = "parameter_id"
                    cursor.execute(
                        f"SELECT value FROM \"{station}\" WHERE {measurement_column}='{measurement_type}' AND observed_at BETWEEN %s AND %s",
                        (timespan['from'], timespan['to'])
                    )
            else:
                cursor.execute(
                    f"SELECT {measurement_type} FROM \"{station}\" WHERE \"{measurement_type}\" IS NOT NULL AND observed_at BETWEEN %s AND %s",
                    (timespan['from'], timespan['to'])
                )

            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting measurement data: {e}")
            return 0
    else:
        print("No connection to database")
        return 0
    
def get_latest_measurement(con):
    """Gets the latest measurement from a specific station"""
    if con is not None:
        try:
            cursor = get_cursor(con)
            results = {}
            cursor.execute(
                f"SELECT humidity FROM public.humidity_data WHERE source='DMI' ORDER BY observed_at DESC LIMIT 1"
            )
            humidity = cursor.fetchone()
            cursor.execute(
                f"SELECT pressure FROM public.pressure_data WHERE source='DMI' ORDER BY observed_at DESC LIMIT 1"
            )
            pressure = cursor.fetchone()
            cursor.execute(
                f"SELECT temperature FROM public.temperature_data WHERE source='DMI' ORDER BY observed_at DESC LIMIT 1"
            )
            temperature = cursor.fetchone()
            if not isinstance(humidity, tuple):
                humidity = (None,)
            if not isinstance(pressure, tuple):
                pressure = (None,)
            if not isinstance(temperature, tuple):
                temperature = (None,)
            results["DMI"] = {"humidity": humidity[0], "pressure": pressure[0], "temperature": temperature[0]}
            
            cursor.execute(
                f"SELECT humidity FROM public.humidity_data WHERE source='BME280' ORDER BY observed_at DESC LIMIT 1"
            )
            humidity = cursor.fetchone()
            cursor.execute(
                f"SELECT pressure FROM public.pressure_data WHERE source='BME280' ORDER BY observed_at DESC LIMIT 1"
            )
            pressure = cursor.fetchone()
            cursor.execute(
                f"SELECT temperature FROM public.temperature_data WHERE source='BME280' ORDER BY observed_at DESC LIMIT 1"
            )
            temperature = cursor.fetchone()
            if not isinstance(humidity, tuple):
                humidity = (None,)
            if not isinstance(pressure, tuple):
                pressure = (None,)
            if not isinstance(temperature, tuple):
                temperature = (None,)
            results["BME280"] = {"humidity": humidity[0], "pressure": pressure[0], "temperature": temperature[0]}
            
            cursor.execute(
                f"SELECT humidity FROM public.humidity_data WHERE source='DS18B20' ORDER BY observed_at DESC LIMIT 1"
            )
            humidity = cursor.fetchone()
            cursor.execute(
                f"SELECT pressure FROM public.pressure_data WHERE source='DS18B20' ORDER BY observed_at DESC LIMIT 1"
            )
            pressure = cursor.fetchone()
            cursor.execute(
                f"SELECT temperature FROM public.temperature_data WHERE source='DS18B20' ORDER BY observed_at DESC LIMIT 1"
            )
            temperature = cursor.fetchone()
            if not isinstance(humidity, tuple):
                humidity = (None,)
            if not isinstance(pressure, tuple):
                pressure = (None,)
            if not isinstance(temperature, tuple):
                temperature = (None,)
            results["DS18B20"] = {"humidity": humidity[0], "pressure": pressure[0], "temperature": temperature[0]}
            
            cursor.execute(
                f"SELECT humidity FROM public.humidity_data WHERE source='SCD41' ORDER BY observed_at DESC LIMIT 1"
            )
            humidity = cursor.fetchone()
            cursor.execute(
                f"SELECT pressure FROM public.pressure_data WHERE source='SCD41' ORDER BY observed_at DESC LIMIT 1"
            )
            pressure = cursor.fetchone()
            cursor.execute(
                f"SELECT temperature FROM public.temperature_data WHERE source='SCD41' ORDER BY observed_at DESC LIMIT 1"
            )
            temperature = cursor.fetchone()
            if not isinstance(humidity, tuple):
                humidity = (None,)
            if not isinstance(pressure, tuple):
                pressure = (None,)
            if not isinstance(temperature, tuple):
                temperature = (None,)
            results["SCD41"] = {"humidity": humidity[0], "pressure": pressure[0], "temperature": temperature[0]}
            
            return results
        except Exception as e:
            print(f"Error getting latest measurement: {e}")
            return 0
    else:
        print("No connection to database")
        return 0