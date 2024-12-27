from azure.storage.blob import BlobServiceClient
import json

class AzureBlobClient:
    def __init__(self, connection_string, container_name):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)
        if not self.container_client.exists():
            self.container_client.create_container()

    def upload_json(self, blob_name, data):
        blob_client = self.container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        print(f"Uploaded data to blob: {blob_name}")
