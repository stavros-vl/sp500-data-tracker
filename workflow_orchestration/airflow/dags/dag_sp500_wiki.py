from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from helpers.upload_to_gcs import upload_to_gcs
import os
import pandas as pd

# Constants
BUCKET = "sp500-tracker-terrabucket"

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
    'sp500_wiki_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch S&P 500 data from wikipedia and upload to GCS',
    schedule_interval='@monthly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    task_wikipedia_to_gcs = PythonOperator(
        task_id='wikipedia_to_gcs',
        python_callable=wikipedia_to_gcs,
        op_args=[BUCKET],
    )

    task_create_bigquery_table = create_bigquery_table(
        bucket_name=BUCKET,
        object_name='sp500_wiki_data_20240420.csv',
        dataset_name='sp500_tables',  # Replace with your dataset name
        table_name='sp500_wiki_data',  # Replace with your table name
        dag=dag,
    )

    task_wikipedia_to_gcs