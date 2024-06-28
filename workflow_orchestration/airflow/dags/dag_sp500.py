from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from helpers.upload_to_gcs import upload_to_gcs
import os
import pandas as pd
import yfinance as yf

# Constants
BUCKET = "sp500-tracker-terrabucket"

# Function to fetch data from Yahoo Finance and upload to GCS
def yfinance_to_gcs(bucket_name, start_date, end_date):
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    sp500_tickers = pd.read_html(url)[0]
    sp500_symbols = sp500_tickers.Symbol.to_list()
    sp500_data = {}
    for symbol in sp500_symbols:
        try:
            data = yf.download(symbol, start=start_date, end=end_date)
            sp500_data[symbol] = data
            print(f"Downloaded data for {symbol}")
        except Exception as e:
            print(f"Error downloading data for {symbol}: {str(e)}")
    sp500_df = pd.concat(sp500_data.values(), keys=sp500_data.keys(), names=['Ticker'])
    date = datetime.now().strftime('%Y%m%d')
    local_file_path = f'sp500_finance_data_{date}.csv.gz'
    sp500_df.to_csv(local_file_path)
    object_name = f'sp500_finance_data_{date}.csv.gz'
    upload_to_gcs(bucket_name, object_name, local_file_path)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'sp500_yfinance_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch S&P 500 data from wikipedia and upload to GCS',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    task_yfinance_to_gcs = PythonOperator(
        task_id='yfinance_to_gcs',
        python_callable=yfinance_to_gcs,
        op_args=[BUCKET, '2024-01-01', (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')],
    )

    task_yfinance_to_gcs
