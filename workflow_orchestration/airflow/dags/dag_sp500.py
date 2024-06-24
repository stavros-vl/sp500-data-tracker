from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from google.cloud import storage
import os
import pandas as pd
import yfinance as yf

# Constants
BUCKET = "sp500-tracker-terrabucket"

# Helper function to upload to GCS
def upload_to_gcs(bucket, object_name, local_file):
    """
    Uploads a file to Google Cloud Storage.
    
    Args:
        bucket (str): The name of the bucket.
        object_name (str): The name of the object in the bucket.
        local_file (str): The local file path to upload.
    """
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

# Function to fetch data from Wikipedia and upload to GCS
def wikipedia_to_gcs(bucket_name):
    """
    Scrapes S&P 500 ticker data from Wikipedia and uploads it to Google Cloud Storage.

    Args:
        bucket_name (str): The name of the Google Cloud Storage bucket to upload the data to.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    sp500_tickers = pd.read_html(url)[0]
    date = datetime.now().strftime('%Y%m%d')
    local_file_path = 'sp500_wiki_data.csv'
    sp500_tickers.to_csv(local_file_path, index=False)
    upload_to_gcs(bucket_name, f'sp500_wiki_data_{date}.csv', local_file_path)

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
    'sp500_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch and upload S&P 500 data to GCS',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    task_wikipedia_to_gcs = PythonOperator(
        task_id='wikipedia_to_gcs',
        python_callable=wikipedia_to_gcs,
        op_args=[BUCKET],
    )

    task_yfinance_to_gcs = PythonOperator(
        task_id='yfinance_to_gcs',
        python_callable=yfinance_to_gcs,
        op_args=[BUCKET, '2024-01-01', (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')],
    )

    task_wikipedia_to_gcs >> task_yfinance_to_gcs
