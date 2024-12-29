import os
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from utils.logger_setup import setup_logger

logger = setup_logger(__name__)


class BlobServiceClientHandler:
    _instance = None

    @classmethod
    def get_instance(cls) -> BlobServiceClient:
        if cls._instance is None:
            #connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
            if not connection_string:
                raise ValueError("Environment variable AZURE_STORAGE_CONNECTION_STRING is not set.")
            try:
                cls._instance = BlobServiceClient.from_connection_string(connection_string)
                logger.info("BlobServiceClient initialized successfully.")
            except AzureError as e:
                raise Exception(f"Failed to initialize BlobServiceClient: {e}")
        return cls._instance