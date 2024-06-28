from google.cloud import storage

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
