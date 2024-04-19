import os
import pandas as pd
from datetime import datetime
from google.cloud import storage

BUCKET = os.environ.get("GCP_GCS_BUCKET", "sp500-tracker-terrabucket")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def wikipedia_to_gcs():

    # Get s&p500 info from wikipedia
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    # Get all the data of S&P 500 tickers in a dataframe
    sp500_tickers = pd.read_html(url)[0]

    
    
