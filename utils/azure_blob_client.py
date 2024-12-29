from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient
import json

from utils.azure_blob import BlobServiceClientHandler
from utils.logger_setup import setup_logger

logger = setup_logger(__name__)

class AzureBlobClient:
    def __init__(self):
        try:
            self.blob_service_client = BlobServiceClientHandler.get_instance()
        except Exception as e:
            logger.error(f"Error during blob service client: {e}")

    def initialize_container(self, container_name):
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            # Check if the container exists
            if not container_client.exists():
                self.blob_service_client.create_container(container_name)
        except Exception as e:
            logger.error(f"Error during container initialization: {e}")

    def upload_data_to_blob(self, container_name: str, blob_name: str, json_data):
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob=blob_name)
            json_string = json.dumps(json_data, indent=4)
            blob_client.upload_blob(json_string, overwrite=True)
            logger.info(f"Successfully uploaded JSON data to {blob_name} in container {container_client.container_name}")
        except AzureError as e:
            logger.error(f"Failed to upload JSON data to {container_client.container_name}/{blob_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"An error occurred while uploading to Blob Storage: {e}")
            raise