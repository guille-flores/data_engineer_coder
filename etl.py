import psycopg2
import os
from dotenv import load_dotenv
import datetime
import pandas as pd
from etl_functions import *


def main():
	load_dotenv()

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
	yesterday = (current_time_utc-datetime.timedelta(days = 1)).strftime('%Y-%m-%d')
	df_stocks = get_polygon_financial_data(POLYGON_BEARER_TOKEN, yesterday)

	# renaming the columns and changing unixtime stamps to a date so we cna read it easily. We also add the current date as ingestion time.
	df_stocks = polygon_financial_df_transformation(df_stocks)
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

	# A connection is an object that represents a session with the PostgreSQL database
	db_conn = redshift_db_connection(AWS_REDSHIFT_DB, AWS_REDSHIFT_USERNAME,  AWS_REDSHIFT_HOST,  AWS_REDSHIFT_PORT,  AWS_REDSHIFT_PASSWORD)
	with db_conn as db_conn:
		# A cursor is an object that allows you to interact with the database through the established connection.
		db_cursor = db_conn.cursor()
		with db_cursor as cur:
			try:
				# Creating a new table
				table_name = 'us_stock_prices'
				redshift_db_create_table(cursor=cur, table=table_name)
				
				# Inserting the records form the dataframe
				redshift_table_data_insert(connection=db_conn, cursor=cur, table=table_name, df=df_stocks)
				
				# Querying a few records
				redshift_top_query(cursor=cur, table=table_name, cols=','.join(list(df_stocks.columns)), toplimit=10)

			except (Exception, psycopg2.DatabaseError) as error:
				print(error)