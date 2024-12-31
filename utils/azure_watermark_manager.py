from azure.functions import HttpResponse
from transitions import Machine, State

from config.config import AzureConfig
from utils.azure_blob_client import AzureBlobClient
from utils.azure_subscription_client import AzureSubscriptionClient
from utils.logger_setup import setup_logger
from utils.save_response import generate_filename, get_subscription_path_container_name, \
    get_resource_path_container_name
from datetime import datetime
import json

logger = setup_logger(name="AzureWatermarkManager")

class AzureWatermarkManager:
    def __init__(self, container_name, blob_name="azure_watermarks.json"):
        self.container_name = container_name
        self.blob_name = blob_name
        self.blob_service_client = AzureBlobClient().blob_service_client
        self.container_client = self.blob_service_client.get_container_client(container_name)
        self.watermark = self._load_watermark()

    def _load_watermark(self):
        try:
            blob_client = self.container_client.get_blob_client(self.blob_name)
            # Check if the blob exists
            if not blob_client.exists():
                logger.warning(f"Watermark blob {self.blob_name} does not exist. Defaulting to None.")
                return None

            # Read the blob data
            blob_data = blob_client.download_blob().readall()
            return json.loads(blob_data).get("resource_last_execution")

        except Exception as e:
            logger.warning(f"Error loading watermark: {e}. Defaulting to None.")
            return None

    def _save_watermark(self):
        try:
            blob_client = self.container_client.get_blob_client(self.blob_name)
            data = {"resource_last_execution": self.watermark}
            blob_client.upload_blob(json.dumps(data, indent=4), overwrite=True)
        except Exception as e:
            logger.error(f"Error saving watermark: {e}")

    def get_watermark(self):
        return self.watermark

    def update_watermark(self, new_watermark):
        self.watermark = new_watermark
        self._save_watermark()

    def filter_changes(self, changes):
        watermark = self.get_watermark()
        if not watermark:
            return changes
        watermark_time = datetime.fromisoformat(watermark)
        filtered_changes = [
            change for change in changes
            if datetime.fromisoformat(change['timestamp'].replace("Z", "")) > watermark_time
        ]
        return filtered_changes

    def merge_resources(self, existing_resources, new_resources):
        """
        Compare and merge existing resources with new resources.
        :param existing_resources: List of existing resource dictionaries.
        :param new_resources: List of new resource dictionaries.
        :return: Merged list of resources.
        """
        existing_resources_dict = {res["id"]: res for res in existing_resources}
        for new_res in new_resources:
            resource_id = new_res["id"]
            if resource_id in existing_resources_dict:
                # Update the existing resource with new data
                existing_resources_dict[resource_id].update(new_res)
            else:
                # Add the new resource
                existing_resources_dict[resource_id] = new_res

        # Return the merged resources as a list
        return list(existing_resources_dict.values())