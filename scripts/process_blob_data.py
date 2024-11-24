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

    df = pd.read_csv(download_stream)


    df_cleaned = df.drop_duplicates()

    
    if 'date' in df.columns:
        df_cleaned['date'] = pd.to_datetime(df_cleaned['date'], errors='coerce')


    df_cleaned.fillna({'column_name': 'Unknown'}, inplace=True)  
    df_cleaned.dropna(inplace=True)  


    if 'date' in df_cleaned.columns:
        df_cleaned['year'] = df_cleaned['date'].dt.year
        df_cleaned['month'] = df_cleaned['date'].dt.month

 
    df_cleaned.rename(columns={'old_column_name': 'new_column_name'}, inplace=True)


    processed_filename = f"processed_{blob.name}"
    df_cleaned.to_csv(processed_filename, index=False)


    curated_blob_client = curated_container_client.get_blob_client(processed_filename)
    with open(processed_filename, "rb") as data:
        curated_blob_client.upload_blob(data, overwrite=True)


    os.remove(processed_filename)

    print(f"Processed and uploaded: {processed_filename}")