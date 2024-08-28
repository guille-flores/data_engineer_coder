from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl import main


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="polygon_stock_market_lastday_data_ingestion",
    description='DAG to run the Python Script that inserts US Stock Market data from the day before.',
    start_date=datetime(2024, 8, 26),
    schedule="@daily",
    catchup=False
) as de_stockmarket_dag:
    python_ingestion_run = PythonOperator(
        task_id='polygon_stock_yesterdays_data_ingestion',
        python_callable=main,
        dag=de_stockmarket_dag
    )

python_ingestion_run