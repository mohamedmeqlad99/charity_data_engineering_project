def upload_to_blob_append(local_new_file, blob_subpath, local_temp_path):
    """
    Append new data to an existing blob. Downloads existing content if present,
    appends new data, and uploads the combined result without needing to delete the blob first.
    Only deletes local temporary files.
    """
    if not AZURE_CONN_STR:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING is not set in your .env file.")
    if not os.path.exists(local_new_file):
        raise FileNotFoundError(f"File does not exist: {local_new_file}")

    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_subpath)

    # Step 1: Download existing blob if exists
    existing_content = []
    try:
        download_stream = blob_client.download_blob()
        existing_content = download_stream.readall().decode('utf-8').splitlines()
        print(f"⬇️ Downloaded existing blob {blob_subpath}")
    except ResourceNotFoundError:
        print(f"⚠️ Blob {blob_subpath} does not exist. Will create new.")

    # Step 2: Read new content
    with open(local_new_file, "r", encoding='utf-8') as f:
        new_content = f.read().splitlines()

    # Step 3: Merge content
    combined_content = []
    
    if existing_content:
        combined_content.extend(existing_content)
        # Skip header if it exists in new content
        if len(new_content) > 1:
            combined_content.extend(new_content[1:])
        else:
            combined_content.extend(new_content)
    else:
        combined_content = new_content

    # Step 4: Create temp file with merged content
    with open(local_temp_path, "w", encoding='utf-8') as f:
        f.write('\n'.join(combined_content))

    # Step 5: Upload directly with overwrite=True (no need to delete first)
    with open(local_temp_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"✅ Uploaded combined CSV to {blob_subpath}")

    # Step 6: Clean up local files only
    try:
        os.remove(local_new_file)
        print(f"🧹 Deleted local file: {local_new_file}")
    except OSError as e:
        print(f"⚠️ Could not delete local file {local_new_file}: {e}")