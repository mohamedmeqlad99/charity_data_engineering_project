import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


connection_string = 'your_connection_string_here'
container_name = 'raw'


local_path = '/home/meqlad/pr/charity_data_engineering_project/data'


blob_service_client = BlobServiceClient.from_connection_string(connection_string)


container_client = blob_service_client.get_container_client(container_name)


for root, dirs, files in os.walk(local_path):
    for file in files:
        
        file_path = os.path.join(root, file)
        blob_client = container_client.get_blob_client(file)

        
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            print(f"Uploaded: {file}")
