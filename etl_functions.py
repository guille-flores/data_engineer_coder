import psycopg2
import requests
import datetime
import pandas as pd

###########################################
# POLYGON FINANCIAL API GET REQUEST 
###########################################
def get_polygon_financial_data(POLYGON_BEARER_TOKEN, date):
    try:
        # obtaining the API call for the stock values per day
        stock_request = requests.get(
            'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/' + date,
            headers={"Authorization":"Bearer " + POLYGON_BEARER_TOKEN}
        )
        
        # request response either OK or not
        if stock_request.status_code == 200:
            print(f"Successfully retrieved data from Polygon API for the date '{date}'.\n")
            # We can have an OK status but have no data to ingest, if that's the case, we will return an empty DF
            if stock_request.json()["resultsCount"] == 0:
                return pd.DataFrame() #empty DF
            stock_results = stock_request.json()["results"]

            # Returning the data as a pandas Data Frame
            return pd.DataFrame(stock_results)
        else:
            print(f"There was an error trying to retrieve data from Polygon API.\n")
            print(f"API Request Status Code: {stock_request.status_code}")
            print(stock_request.json())
    except Exception as error:
        print(f"Unable to request/retrieve data from the Polygon API for the desired date '{date}'.\n")
        print(error)

def polygon_financial_df_transformation(df):
    try:
        # The API data will need to be processed (data type manipulation, selecting columns, etc.)
        # Selecting just the needed columns
        df = df[['T', 'v', 'h', 'l', 't']]

        # renaming the columns so we can understand their meaning/what data they have
        df = df.rename(columns={
            'T':'stock',
            'v':'volume',
            'h':'highest_price',
            'l':'lowest_price',
            't':'request_unix_timestamp'
        })

        # changing volume to integer
        df['volume'] = df['volume'].astype(int)
        # parsing unix timestamp to a human readable date. Unixtimestamp for Polygon is coming in miliseconds (ms)
        df['request_timestamp'] = pd.to_datetime(df['request_unix_timestamp'], unit='ms')

        # we will also ingest the current timestamp when we obtained and ingested the data by this script
        current_time_utc = datetime.datetime.now(datetime.UTC)
        df['ingestion_timestamp'] = current_time_utc
        df['ingestion_unix_timestamp'] = round(current_time_utc.timestamp()*1000)
        return df
    except Exception as error:
        print(f"There was an error when trying to transform/manipulate the Polygon Financial API data.\n")
        print(error)

###########################################
# REDSHIFT CONNECTION 
###########################################
def redshift_db_connection(AWS_REDSHIFT_DB, AWS_REDSHIFT_USERNAME,  AWS_REDSHIFT_HOST,  AWS_REDSHIFT_PORT,  AWS_REDSHIFT_PASSWORD):
    try:
        conn = psycopg2.connect(
            dbname=AWS_REDSHIFT_DB,
            user=AWS_REDSHIFT_USERNAME, 
            host=AWS_REDSHIFT_HOST, 
            port=AWS_REDSHIFT_PORT, 
            password=AWS_REDSHIFT_PASSWORD
        )
        print(f"Successfully connected to DB '{AWS_REDSHIFT_DB}'\n")
        return conn
    except Exception as error:
        print(f"Unable to connect to DB named '{AWS_REDSHIFT_DB}'.\n")
        print(error)



###########################################
# REDSHIFT TABLE CREATION 
###########################################
def redshift_db_create_table(cursor, table):
    try:
        # Creating the table to ingest the data. Some rules/considerations we applied:
        # 1. Stock Name CANNOT be null
        # 2. Field data type... volume as integer, price as decimal, timestamp as BIGINT, etc.
        # 3. PK: composite primary key using stock name and timestamp... as the same stock cannot be in the table more than once for the same time period
        cursor.execute("""
            CREATE TABLE if not exists 
                {table} (stock varchar(50) not null, volume int, highest_price decimal, lowest_price decimal, request_unix_timestamp BIGINT, request_timestamp timestamp not null, ingestion_unix_timestamp  BIGINT, ingestion_timestamp timestamp, PRIMARY KEY (stock, request_timestamp))
        """.format(table=table))
        print(f"Successfully created the table '{table}'.\n")
    except Exception as error:
        print(f"Unable to create table '{table}'.\n")
        print(error)



###########################################
# REDSHIFT TABLE DATA INSERTION - DUPLICATE RISK
###########################################
# THIS FUNCTION WILL APPEND RECORDS (RISK OF DUPLICATES IN TABLE) - USE THE FUNCTION redshift_table_upsert TO AVOID DUPLICATES
def redshift_table_data_insert(connection, cursor, table, df):
    try:
        # Inserting records to the created table. Column names are joined by a comma to have a text "col1, col2, col3, col4" so we cna use it for SQL.
        cols = ','.join(list(df.columns))

        # Changing Datetime to strings, otherwise the to_numpy() function will show the date surrounded by the class/object type: Timestamp('2024-08-01 20:00:00') instead of '2024-08-01 20:00:00'
        df['request_timestamp'] = df['request_timestamp'].astype(str)
        df['ingestion_timestamp'] = df['ingestion_timestamp'].astype(str)
        data_tuple = [tuple(x) for x in df.to_numpy()]

        query = """
            INSERT INTO {table} ({columns})
            VALUES {data}; 
        """.format(table=table, columns = cols, data=str(data_tuple)[1:-1])
        # Tuple to string is shown as '[(tuple 1), (tuple 2), ...]' we remove the [] as the INSERT command only ask for 'VALUES (tuple 1), (tuple 2), ( tuple 3)...;'

        cursor.execute(query)
        # the cursor will store information of the command we ran, the SQL query in this case, so we can know how many rows were inserted and compared them vs initial DF size
        print(f'Successfully inserted {cursor.rowcount}/{df.shape[0]} rows into the table "{table}".\n')
        # in case the inserted row are less than the DF size, it means some rows were not inserted and we want to alert the user so they can take a look.
        if cursor.rowcount < df.shape[0]:
            print(f'WARNING: {df.shape[0]-cursor.rowcount} rows were not successfully inserted, please take a look.\n')
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error: %s" % error) 
        connection.rollback()



###########################################
# REDSHIFT TABLE DATA UPSERT 
###########################################
def redshift_table_upsert(connection, cursor, table, staging_table, cols):
    try:
        # Counting the number of rows in the table in redshift, so we can know how many records were inserted or updated
        query_count = """
            SELECT 
                COUNT(*)
            FROM {table};
        """.format(table=table)
        cursor.execute(query_count)
        table_count = cursor.fetchone() # value of the COUNT result is in a row, the only row from the query performed
        table_count = table_count[0] # the fetchone method returns a tuple "(count,)"... we want the first value as that's were the result of the COUNT is
        print(f'There are {table_count} rows in table "{table}".\n')

        # Counting the number of rows to insert
        query_count = """
            SELECT 
                COUNT(*)
            FROM {staging_table};
        """.format(staging_table=staging_table)
        cursor.execute(query_count)
        staging_count = cursor.fetchone()
        staging_count = staging_count[0]
        print(f'There are {staging_count} rows to insert from table "{staging_table}" into "{table}".\n')

        # deleting existing rows to update these records, we use as compund primary keys the stock name and timestamp
        query_delete = """
            DELETE FROM {table}
            USING {staging_table}
            WHERE {table}.stock = {staging_table}.stock
                AND {table}.request_timestamp = {staging_table}.request_timestamp;
        """.format(table=table, staging_table=staging_table, columns = cols)
        cursor.execute(query_delete)
        if cursor.rowcount > 0:
            print(f'Successfully deleted {cursor.rowcount}/{table_count} rows from table "{table}".\n')

        # Inserting all data from staging table
        query_insert = """
            INSERT INTO {table} ({columns})
            SELECT 
                {columns}
            FROM {staging_table};
        """.format(table=table, staging_table=staging_table, columns = cols)
        cursor.execute(query_insert)
        inserted = cursor.rowcount
        print(f'Successfully inserted {inserted}/{staging_count} rows from table "{staging_table}" into "{table}".\n')
        
        if inserted < staging_count:
            print(f'WARNING: {staging_count-inserted} rows from "{staging_table}" were not successfully inserted into "{table}", please take a look.\n')
        else:
            # Clearing the staging table
            query_empty = """
                TRUNCATE TABLE {staging_table};
            """.format(staging_table=staging_table)
            cursor.execute(query_empty)
            print(f'Successfully emptied the staging table "{staging_table}".\n')
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error: %s" % error) 
        connection.rollback()


###########################################
# REDSHIFT TABLE QUERY TOP 10 
###########################################
def redshift_top_query(cursor, table, cols, toplimit):
    try:
        query = """
            SELECT TOP {top}
                {columns}
            FROM {table}; 
        """.format(table=table, columns = cols, top = toplimit)

        cursor.execute(query)
        print(f'Top {toplimit} records from {table}:')
        for record in cursor:
            print(record)
    except Exception as error:
        print(f"Unable to retrieve records from table '{table}'.\n")
        print(error)