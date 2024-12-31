import json
from datetime import datetime

from azure.functions import HttpResponse
from transitions import Machine, State

from config.config import AzureConfig
from utils.azure_blob_client import AzureBlobClient
from utils.azure_subscription_client import AzureSubscriptionClient
from utils.azure_watermark_manager import AzureWatermarkManager
from utils.handle_error import handle_errors
from utils.logger_setup import setup_logger
from utils.save_response import generate_filename, get_subscription_path_container_name, \
    get_resource_path_container_name

logger = setup_logger(name="AzureWorkflow")

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
        self.config = AzureConfig().get_config()

        self.blob_client = AzureBlobClient()
        container_name = self.config["container_name_azure"]
        root_folder_name = self.config["folder_raw_data"]
        self.subscription_path_container_name = get_subscription_path_container_name(root_folder_name=root_folder_name, container_name=container_name)
        self.resource_path_container_name = get_resource_path_container_name(root_folder_name=root_folder_name, container_name=container_name)
        self.blob_client.initialize_container(container_name)
        self.subscription_client = AzureSubscriptionClient()
        self.subscriptions = []
        self.resources = []
        self.empty_resource_subscriptions = []
        self.records_per_page = self.config["records_per_page"]

        # Initialize watermark manager
        self.watermark_manager = AzureWatermarkManager(container_name=container_name)

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

        # noinspection PyUnresolvedReferences
        self.start_workflow()  # Trigger the next state event

    @handle_errors
    def on_fetch_subscriptions(self):

        logger.info("Fetching subscriptions...")

        # Logic to fetch subscriptions
        try :
            self.subscriptions_data = self.subscription_client.fetch_subscriptions()

            if not self.subscriptions_data:

                logger.warning("No subscription data found or the request failed.")

                raise ValueError("No subscription data found or the request failed.")

            logger.info("Fetched subscription data successfully.")

            # noinspection PyUnresolvedReferences
            self.fetch_subscriptions_done() # Trigger the next state event

        except Exception as e:
            raise RuntimeError(f"Error in fetch subscriptions: {str(e)}") from e

    @handle_errors
    def on_upload_subscriptions(self):

        logger.info("Uploading subscriptions...")

        # Upload subscription data to Azure Blob Storage
        formatted_response = {
            "value": self.subscriptions_data
        }

        logger.info(f"Uploading subscriptions_data Object")

        try:

            self.blob_client.upload_data_to_blob(
                container_name=self.subscription_path_container_name,
                blob_name=generate_filename('subscription'),
                json_data=formatted_response)

            # noinspection PyUnresolvedReferences
            self.upload_subscriptions_done()  # Trigger the next state event

        except Exception as e:
            logger.error(f"Failed to upload subscription data to Blob Storage: {str(e)}")
            raise RuntimeError(f"Error uploading subscription data: {str(e)}") from e

    @handle_errors
    def on_fetch_resources(self):

        logger.info("Fetching resources...")

        # Fetch the last execution time from the watermark
        last_execution_time = self.watermark_manager.get_watermark()

        time_diff_hours = None

        if last_execution_time:
            last_execution_time = datetime.fromisoformat(last_execution_time)
            time_diff_hours = (datetime.utcnow() - last_execution_time).total_seconds() / 3600

            logger.info(f"Last execution time: {last_execution_time}, fetching changes since {time_diff_hours} hours.")

        else:
            logger.info("No previous execution time found. Fetching all resources.")

        # Update the current execution time in the watermark
        current_execution_time = datetime.utcnow()

        # Process resources for each subscription
        for subscription in self.subscriptions_data:

            subscription_id = subscription.get("subscription_id")

            if not subscription_id:
                logger.warning("Subscription ID is null or empty.")
                continue

            logger.info(f"Fetching resources for subscription: {subscription_id}")

            try:
                # Fetch resources for the given subscription with pagination
                for page_number, resources_page_data in enumerate(
                        self.subscription_client.get_resources_for_subscription_paginated(subscription_id, self.records_per_page, time_diff_hours), start=1):

                    logger.info(f"Fetched resources for subscription ID: {subscription_id} successfully.")

                    if not resources_page_data:
                        logger.warning(f"No resource data found for subscription ID: {subscription_id}. Skipping.")

                        self.empty_resource_subscriptions.append(subscription_id)
                        continue  # Skip to the next iteration if no resources are found

                    try:
                        # Generate a unique blob name with page number
                        blob_name = generate_filename(f'resource_{subscription_id}', page_number=page_number)

                        try:
                            existing_blob_data = self.blob_client.read_blob_file(
                                container_name=self.resource_path_container_name,
                                blob_name=blob_name
                            )

                            if existing_blob_data is not None:

                                existing_resources = existing_blob_data['value']

                                # Compare and merge resources
                                merged_resources = self.watermark_manager.merge_resources(existing_resources, resources_page_data)

                            else:
                                logger.info("Blob file does not exist or could not be read; replace merged_resources with recent data.")
                                merged_resources = resources_page_data

                        except Exception as e:
                            logger.info(f"Error while fetching existing resource data for blob: {blob_name}. Starting fresh.")
                            raise RuntimeError(f"Error while fetching existing resource data: {str(e)}") from e

                        formatted_response = {"value": merged_resources}

                        # Upload the current page to Blob Storage
                        self.blob_client.upload_data_to_blob(
                            container_name=self.resource_path_container_name,
                            blob_name=blob_name,
                            json_data=formatted_response
                        )
                        logger.info(f"Page {page_number} uploaded successfully for subscription ID {subscription_id}.")

                    except Exception as e:
                        logger.error(f"Failed to fetch resource data: {str(e)}")
                        raise RuntimeError(f"Error fetching resource data: {str(e)}") from e

                # Update the watermark with the current execution time
                self.watermark_manager.update_watermark(current_execution_time.isoformat())

                # Log subscriptions with empty resource responses at the end
                if self.empty_resource_subscriptions:
                    logger.info("subscriptions with empty resource responses")
                    for index, value in enumerate(self.empty_resource_subscriptions, start=1):
                        logger.info(f"{index}: {value}")

                # noinspection PyUnresolvedReferences
                self.fetch_resources_done()

            except Exception as e:
                raise RuntimeError(f"Error in fetch resources: {str(e)}") from e

    @handle_errors
    def on_upload_resources(self):
        # noinspection PyUnresolvedReferences
        self.upload_resources_done()  # Trigger the next state event

    def on_end(self):
        logger.info("Workflow completed successfully!")
