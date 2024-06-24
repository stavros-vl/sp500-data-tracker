from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
import logging
import os

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "default-project-id")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "default-credentials")

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 23),
    'retries': 0,
}

# Define the DAG
dag = DAG(
    'gcs_upload_test',
    default_args=default_args,
    description='A simple DAG to upload a dummy file to GCS using a custom function',
    schedule_interval=None,
)

# Python function to create a dummy file
def create_dummy_file():
    dummy_content = "This is a test file."
    file_path = '/tmp/dummy_file.txt'
    with open(file_path, 'w') as f:
        f.write(dummy_content)
    logging.info(f"Dummy file created at {file_path}")

# Custom function to upload to GCS
def upload_to_gcs(bucket, object_name, local_file):
    """
    Uploads a file to Google Cloud Storage.
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python

    Args:
        bucket (str): The name of the bucket.
        object_name (str): The name of the object in the bucket.
        local_file (str): The local file path to upload.
    """
    logging.info(f"Uploading {local_file} to {bucket}/{object_name}")
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    logging.info(f"File {local_file} uploaded to {object_name} in bucket {bucket.name}")

# Wrapper function to call the upload_to_gcs function
def upload_to_gcs_task():
    upload_to_gcs('sp500-tracker-terrabucket', 'dummy_file.txt', '/tmp/dummy_file.txt')

# Debug function to print environment variables
def print_env_vars():
    logging.info("Environment variables:")
    for key, value in os.environ.items():
        logging.info(f"{key}: {value}")

# Task to create the dummy file
create_file_task = PythonOperator(
    task_id='create_dummy_file',
    python_callable=create_dummy_file,
    dag=dag,
)

# Task to upload the dummy file to GCS
upload_file_task = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs_task,
    dag=dag,
)

# Task to print environment variables for debugging
print_env_vars_task = PythonOperator(
    task_id='print_env_vars',
    python_callable=print_env_vars,
    dag=dag,
)

# Set task dependencies
create_file_task >> print_env_vars_task >> upload_file_task


