import os
from azure.storage.blob import BlobServiceClient
import pandas as pd
from dotenv import load_dotenv


load_dotenv()


connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")


blob_service_client = BlobServiceClient.from_connection_string(connection_string)


raw_container_client = blob_service_client.get_container_client("raw")
curated_container_client = blob_service_client.get_container_client("curated")


blob_list = raw_container_client.list_blobs()


for blob in blob_list:

    raw_blob_client = raw_container_client.get_blob_client(blob.name)
    download_stream = raw_blob_client.download_blob()