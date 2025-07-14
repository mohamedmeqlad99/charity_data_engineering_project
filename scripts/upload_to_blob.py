import os
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import hashlib

# Load .env
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
DIR = os.getenv("DIR")

if not AZURE_CONN_STR or not CONTAINER_NAME or not DIR:
    raise ValueError("Missing environment variables. Check .env file.")

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

def get_md5(content: str) -> str:
    return hashlib.md5(content.encode("utf-8")).hexdigest()

def upload_to_blob(filepath: str, blob_subpath: str):
    blob_client = container_client.get_blob_client(blob=blob_subpath)

    with open(filepath, "r", encoding="utf-8") as f:
        new_data = f.read().strip()
    new_md5 = get_md5(new_data)

    if blob_client.exists():
        existing_data = blob_client.download_blob().readall().decode("utf-8").strip()
        if get_md5(existing_data) == new_md5:
            print(f"⚠️ Skipped: {filepath} (identical content)")
            return

        backup_name = f"{blob_subpath.rsplit('.', 1)[0]}_backup_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.csv"
        container_client.upload_blob(backup_name, existing_data.encode("utf-8"))

        merged_data = existing_data + "\n" + new_data
        blob_client.upload_blob(merged_data.encode("utf-8"), overwrite=True)
        print(f"🆗 Appended and backed up: {filepath} → {blob_subpath}")
    else:
        with open(filepath, "rb") as f:
            blob_client.upload_blob(f)
        print(f"✅ Uploaded new: {filepath} → {blob_subpath}")

    log_blob = container_client.get_blob_client("upload_logs/log.txt")
    log_entry = f"[{datetime.now(timezone.utc).isoformat()}] Uploaded: {blob_subpath}\n"

    if log_blob.exists():
        existing_log = log_blob.download_blob().readall().decode("utf-8")
        log_blob.upload_blob(existing_log + log_entry, overwrite=True)
    else:
        log_blob.upload_blob(log_entry)

def upload_directory(data_dir: str):
    for filename in os.listdir(data_dir):
        if filename.endswith(".csv"):
            local_path = os.path.join(data_dir, filename)
            blob_path = f"{filename.split('.')[0]}/{filename}"
            upload_to_blob(local_path, blob_path)

if __name__ == "__main__":
    upload_directory(DIR)
