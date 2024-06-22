"""
This DAG will fetch S&P500 data from the yfinance API and Wikipedia and then
upload it to the specified GCS bucket.
"""

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from pendulum import datetime

import logging
import os
import pandas as pd
# import yfinance as yf
from datetime import datetime, timedelta
from google.cloud import storage

# Ensure GOOGLE_APPLICATION_CREDENTIALS is set
if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

BUCKET = os.environ.get("GCP_GCS_BUCKET", "sp500-tracker-terrabucket")

@dag(
    start_date=datetime(2024, 6, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "airflow", "retries": 1},
    tags=["s&p500-ingestion"],
)

def data_ingestion_gcs_dag():
    @task
    def wikipedia_to_gcs(bucket_name):
        """
        Scrapes S&P 500 ticker data from Wikipedia and uploads it to Google Cloud Storage.

        Args:
            bucket_name (str): The name of the Google Cloud Storage bucket to upload the data to.
        """
        # Get s&p500 info from wikipedia
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

        # Get all the data of S&P 500 tickers in a dataframe
        sp500_tickers = pd.read_html(url)[0]

        # Get current date
        date = datetime.now().strftime('%Y%m%d')

        # Save to local csv
        local_file_path = 'sp500_wiki_data.csv'
        sp500_tickers.to_csv(local_file_path, index=False)

        # Check if the file is correctly saved
        if os.path.exists(local_file_path):
            logging.info(f"File {local_file_path} saved successfully with content:\n{pd.read_csv(local_file_path).head()}")

        # Upload to GCS
        upload_to_gcs(bucket_name, f'sp500_wiki_data_{date}.csv', 'sp500_wiki_data.csv')

    @task
    def upload_to_gcs(bucket, object_name, local_file):
        """
        Uploads a file to Google Cloud Storage.
        Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
        
        Args:
            bucket (str): The name of the bucket.
            object_name (str): The name of the object in the bucket.
            local_file (str): The local file path to upload.
        """
        client = storage.Client()
        bucket = client.bucket(bucket)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_file)
        logging.info(f"File {local_file} uploaded to {object_name} in bucket {bucket.name}")

    

    upload_task = wikipedia_to_gcs(BUCKET)

data_ingestion_gcs_dag()