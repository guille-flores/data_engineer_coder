import psycopg2
import os
from dotenv import load_dotenv
import datetime
import pandas as pd
from utils.etl_functions import *


def main():
	try:
		# Get the directory of the current script (etl.py)
		current_dir = os.path.dirname(__file__)
		# Construct the path to the project root by going up one level from src/
		project_root = os.path.abspath(os.path.join(current_dir, '..'))
		# Build the full path to the .env file in the config directory
		dotenv_path = os.path.join(project_root, 'config', '.env')
		load_dotenv(dotenv_path)

		###########################################################################################
		###########################################################################################
		# CONNECTING TO POLYGON API TO RETRIEVE THE DATA AND MANIPULATE IT
		###########################################################################################
		###########################################################################################

		# Connecting to Polygon API to retrieve financial data
		# Doc: https://polygon.io/docs/stocks/get_v2_aggs_grouped_locale_us_market_stocks__date
		POLYGON_BEARER_TOKEN = os.getenv('POLYGON_BEARER_TOKEN')

		# Polygon API is a free API, we cannot retrieve real-time nor today's data (before market closes). To avoid errors, w eretrieve last day data 
		current_time_utc = datetime.datetime.now(datetime.UTC) 
		# As the time is in UTC, and we are using the US Stock market, we will subtract 1 day and 6 hours (UTC is 6 hours ahead of CST, which is the US timezone for most states)
		yesterday = (current_time_utc-datetime.timedelta(days = 1, hours=6))
		
		# in case the day is weekend, there is no stock market data
    	# we can ask if they want Friday data instead
		if yesterday.weekday() > 4: # 0 = Monday, 4 = Friday, 6 = Sunday
			print('--------------------------------------')
			print('------------  WARNING  ---------------')
			print('--------------------------------------')
			days_to_friday = yesterday.weekday() - 4
			last_friday = (yesterday-datetime.timedelta(days = days_to_friday)).strftime('%Y-%m-%d')
			print(f'Seems like you are requesting data from a Saturday or Sunday ({yesterday.strftime("%Y-%m-%d")}).\nThere is no Stock Market data during those days.')
			print(f'We will request the data from last Friday ({last_friday}) instead.\n')
			yesterday = yesterday-datetime.timedelta(days = days_to_friday)
			''' INTERACTIVE API REQUEST
			answer = ''
			attempts = 0
			max_attempts = 5
			while answer not in ['Y', 'N', 'NO', 'YES', 'S', 'SI', 'SÍ'] and attempts < max_attempts:
				if answer not in ['Y', 'N', 'NO', 'YES', 'S', 'SI', 'SÍ', '']:
					print(f'Only valid answers are \'y\', \'yes\', or \'n\', \'no\'. You answered with \'{answer}\'.')
				answer = input(f'Do you want to request the data from last Friday ({last_friday}) instead?\n[y/n]: ').upper().strip()
				attempts += 1

			# in case user does want to get data from last friday, we will change the date
			if answer not in ['Y', 'YES', 'S', 'SI', 'SÍ'] and attempts == max_attempts:
				print(f'\nYou have reached the maximum number of attempts to specify if you want or not to get data from last Friday ({last_friday}) as you are requesting data for a weekend ({yesterday.strftime("%Y-%m-%d")}).\nRemember that no stock market data is available during weekends. As you reached the maximum numbers of attemps ({max_attempts}) without providing a valid answer (yes/y/s/si, no/n), we will retrieve by default data from last Friday.\n')
			if answer in ['Y', 'YES', 'S', 'SI', 'SÍ'] or attempts == max_attempts:
				yesterday = yesterday-datetime.timedelta(days = days_to_friday)
			'''
        # format the date as needed for the API call of Polygon
		yesterday = yesterday.strftime('%Y-%m-%d')
		
		df_stocks = get_polygon_financial_data(POLYGON_BEARER_TOKEN, yesterday)

		if not isinstance(df_stocks, pd.DataFrame) or df_stocks.empty:
			raise Exception("Sorry, seems like there is no data to work with.")
		# renaming the columns and changing unixtime stamps to a date so we cna read it easily. We also add the current date as ingestion time.
		df_stocks = polygon_financial_df_transformation(df_stocks)
		print("Polygon API Response - Top Rows (DF HEAD)")
		print(df_stocks.head())

		###########################################################################################
		###########################################################################################
		# CONNECTING TO AMAZON AWS REDSHIFT TO INGEST THE API DATA
		###########################################################################################
		###########################################################################################

		# Amazon AWS Redshift Credentials
		AWS_REDSHIFT_USERNAME = os.getenv('AWS_REDSHIFT_USERNAME')
		AWS_REDSHIFT_PASSWORD = os.getenv('AWS_REDSHIFT_PASSWORD')
		AWS_REDSHIFT_HOST = os.getenv('AWS_REDSHIFT_HOST')
		AWS_REDSHIFT_PORT = os.getenv('AWS_REDSHIFT_PORT')
		AWS_REDSHIFT_DB = os.getenv('AWS_REDSHIFT_DB')
		AWS_REDSHIFT_SCHEMA = os.getenv('AWS_REDSHIFT_SCHEMA')

		# A connection is an object that represents a session with the PostgreSQL database
		db_conn = redshift_db_connection(AWS_REDSHIFT_DB, AWS_REDSHIFT_USERNAME,  AWS_REDSHIFT_HOST,  AWS_REDSHIFT_PORT,  AWS_REDSHIFT_PASSWORD)
		with db_conn as db_conn:
			# A cursor is an object that allows you to interact with the database through the established connection.
			db_cursor = db_conn.cursor()
			with db_cursor as cur:
				try:
					# Creating a new table if it doesn't exist
					table_name = 'us_stock_prices'
					table_name = AWS_REDSHIFT_SCHEMA + '.' + table_name
					redshift_db_create_table(cursor=cur, table=table_name)
					# To UPSERT records, we need a staging table to store the retrieved records from the API
					table_name_staging = table_name + '_staging'
					redshift_db_create_table(cursor=cur, table=table_name_staging)
					
					# Inserting the records from the dataframe into the staging table
					redshift_table_data_insert(connection=db_conn, cursor=cur, table=table_name_staging, df=df_stocks)
					
					# Upserting into the main table
					redshift_table_upsert(connection=db_conn, cursor=cur, table=table_name, staging_table=table_name_staging, cols=','.join(list(df_stocks.columns)))

					# Querying a few records and returning them in a pandas DF
					response_df = redshift_top_query(conn=db_conn, cursor=cur, table=table_name, cols=','.join(list(df_stocks.columns)), toplimit=10)
					print(response_df)
				except (Exception, psycopg2.DatabaseError) as error:
					print(error)
	except Exception as error:
		print("There was an error when trying to perform the ETL process.\n")
		print(error)