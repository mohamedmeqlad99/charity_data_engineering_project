import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Environment variables
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "raw"

def upload_to_blob(filepath, blob_subpath):
    """
    Upload a single file to Azure Blob Storage.
    Args:
        filepath (str): Local path to the file (e.g., /data/donations.csv)
        blob_subpath (str): Path inside the container (e.g., donations/donations.csv)
    """
    if not AZURE_CONN_STR:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING is not set in your .env file.")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File does not exist: {filepath}")

    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_subpath)

    with open(filepath, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    print(f"✅ Uploaded {filepath} → blob://{CONTAINER_NAME}/{blob_subpath}")
