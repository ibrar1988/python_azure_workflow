import os
from datetime import time

from azure.core.exceptions import AzureError
from azure.functions import HttpResponse
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest
from transitions import Machine, State

from config.config import AzureConfig
from utils.azure_blob_client import AzureBlobClient
from utils.azure_subscription_client import AzureSubscriptionClient
import json

from utils.logger_setup import setup_logger
from utils.save_response import generate_filename, get_subscription_path_container_name, get_resource_path_container_name

logger = setup_logger(__name__)

class AzureWorkflow:
    states = [
        State(name="start", on_enter="on_start"),
        State(name="fetch_subscriptions", on_enter="on_fetch_subscriptions"),
        State(name="upload_subscriptions", on_enter="on_upload_subscriptions"),
        State(name="fetch_resources", on_enter="on_fetch_resources"),
        State(name="upload_resources", on_enter="on_upload_resources"),
        State(name="end", on_enter="on_end"),
    ]

    def __init__(self):
        self.subscriptions_data = None
        self.machine = None
        #self.config = config
        self.config = AzureConfig().get_config()

        self.blob_client = AzureBlobClient()
        #self.blob_client.initialize_main_container("azure-greenfield")
        container_name = self.config["container_name_azure"]
        root_folder_name = self.config["folder_raw_data"]
        self.subscription_path_container_name = get_subscription_path_container_name(root_folder_name=root_folder_name, container_name=container_name)
        self.resource_path_container_name = get_resource_path_container_name(root_folder_name=root_folder_name, container_name=container_name)
        #root_folder_name:str, container_name
        self.blob_client.initialize_container(container_name)
        self.subscription_client = AzureSubscriptionClient()
        self.subscriptions = []
        self.resources = []
        self.empty_resource_subscriptions = []
        self.records_per_page = self.config["records_per_page"]

        # Initialize the state machine
        self.machine = Machine(
            model=self,
            states=AzureWorkflow.states,
            initial="start",
            transitions=[
                {"trigger": "start_workflow", "source": "start", "dest": "fetch_subscriptions"},
                {"trigger": "fetch_subscriptions_done", "source": "fetch_subscriptions",
                 "dest": "upload_subscriptions"},
                {"trigger": "upload_subscriptions_done", "source": "upload_subscriptions", "dest": "fetch_resources"},
                {"trigger": "fetch_resources_done", "source": "fetch_resources", "dest": "upload_resources"},
                {"trigger": "upload_resources_done", "source": "upload_resources", "dest": "end"},
            ],
        )

    def on_start(self):
        logger.info("Workflow started.")
        #self.blob_client = AzureBlobClient()
        #self.blob_client.initialize_container(self.subscription_path_container_name)
        # noinspection PyUnresolvedReferences
        self.start_workflow()  # Trigger the next state event

    def on_fetch_subscriptions(self):
        logger.info("Fetching subscriptions...")
        # Add your logic to fetch subscriptions here
        self.subscriptions_data = self.subscription_client.fetch_subscriptions()
        if not self.subscriptions_data:
            logger.warning("No subscription data found or the request failed.")
            return HttpResponse(
                "No subscription data found or the request failed.",
                status_code=500
            )
        logger.info("Fetched subscription data successfully.")
        # noinspection PyUnresolvedReferences
        self.fetch_subscriptions_done()
    def on_upload_subscriptions(self):
        logger.info("Uploading subscriptions...")
        # Upload subscription data to Azure Blob Storage
        formatted_response = {
            "value": self.subscriptions_data
        }

        logger.info(f"Uploading subscriptions_data Object")

        try:
            self.blob_client.upload_data_to_blob(container_name=self.subscription_path_container_name, blob_name=generate_filename('subscription'), json_data=formatted_response)
        except Exception as e:
            logger.error(f"Failed to upload subscription data to Blob Storage: {str(e)}")
            return HttpResponse(f"Error uploading subscription data: {str(e)}", status_code=500)

        # noinspection PyUnresolvedReferences
        self.upload_subscriptions_done()  # Trigger the next state event

    def on_fetch_resources(self):
        logger.info("Fetching resources...")
        #self.blob_client = AzureBlobClient()
        #self.blob_client.initialize_container(self.resource_path_container_name)
        # Process resources for each subscription
        for subscription in self.subscriptions_data:
            subscription_id = subscription.get("subscription_id")
            if not subscription_id:
                logger.warning("Subscription ID is null or empty.")
                continue
            logger.info(f"Fetching resources for subscription: {subscription_id}")
            # Fetch resources for the subscription
            for page_number, resources_page_data in enumerate(
                    self.subscription_client.get_resources_for_subscription_paginated(subscription_id, self.records_per_page), start=1):
                logger.info(f"Fetched resources for subscription ID: {subscription_id} successfully.")
                if not resources_page_data:
                    logger.warning(f"No resource data found for subscription ID: {subscription_id}. Skipping.")
                    self.empty_resource_subscriptions.append(subscription_id)
                    continue  # Skip to the next iteration if no resources are found

                formatted_response = {"value": resources_page_data}
                try:
                    # Generate a unique blob name with page number
                    blob_name = generate_filename(f'resource_{subscription_id}', page_number=page_number)

                    # Upload the current page to Blob Storage
                    self.blob_client.upload_data_to_blob(
                        container_name=self.resource_path_container_name,
                        blob_name=blob_name,
                        json_data=formatted_response
                    )
                    logger.info(f"Page {page_number} uploaded successfully for subscription ID {subscription_id}.")
                except Exception as e:
                    logger.error(f"Failed to upload resource data to Blob Storage: {str(e)}")
                    return HttpResponse(f"Error uploading resource data: {str(e)}", status_code=500)

        # Log subscriptions with empty resource responses at the end
        if self.empty_resource_subscriptions:
            logger.info("subscriptions with empty resource responses")
            for index, value in enumerate(self.empty_resource_subscriptions, start=1):
                logger.info(f"{index}: {value}")
        # noinspection PyUnresolvedReferences
        self.fetch_resources_done()

    def on_upload_resources(self):
        logger.info("Uploading resources...")
        # self.blob_client = AzureBlobClient(
        #     self.config["blob_storage_connection_string"],
        #     self.resource_path_container_name
        # )
        # Add your logic to finalize resource uploads here
        # noinspection PyUnresolvedReferences
        self.upload_resources_done()  # Trigger the next state event

    def on_end(self):
        logger.info("Workflow completed.")


    #-------------

    # def fetch_subscriptions(self):
    #     print("Fetching subscriptions...")
    #     self.subscriptions = self.subscription_client.fetch_subscriptions()
    #
    # def upload_subscriptions(self):
    #     print("Uploading subscriptions...")
    #     self.blob_client.upload_json(self.config["subscriptions_blob_name"], self.subscriptions)
    #
    # def fetch_resources(self):
    #     print("Fetching resources...")
    #     for subscription in self.subscriptions:
    #         subscription_id = subscription["id"]
    #         self.resources.extend(self.subscription_client.fetch_resources(subscription_id))
    #
    # def upload_resources(self):
    #     print("Uploading resources...")
    #     self.blob_client.upload_json(self.config["resources_blob_name"], self.resources)
    #
    # def end_process(self):
    #     print("Workflow completed successfully!")
