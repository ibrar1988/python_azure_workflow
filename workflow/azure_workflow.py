from transitions import Machine
from utils.azure_blob_client import AzureBlobClient
from utils.azure_subscription_client import AzureSubscriptionClient
import json


class AzureWorkflow:
    states = ["start", "fetch_subscriptions", "upload_subscriptions", "fetch_resources", "upload_resources", "end"]

    def __init__(self, config):
        self.config = config
        self.blob_client = AzureBlobClient(
            config["blob_storage_connection_string"],
            config["container_name"]
        )
        self.subscription_client = AzureSubscriptionClient()
        self.subscriptions = []
        self.resources = []

        self.machine = Machine(model=self, states=AzureWorkflow.states, initial="start")
        self.machine.add_transition("fetch_subscriptions", "start", "fetch_subscriptions",
                                    after="fetch_subscriptions_data")
        self.machine.add_transition("upload_subscriptions", "fetch_subscriptions", "upload_subscriptions",
                                    after="upload_subscriptions_data")
        self.machine.add_transition("fetch_resources", "upload_subscriptions", "fetch_resources",
                                    after="fetch_resources_data")
        self.machine.add_transition("upload_resources", "fetch_resources", "upload_resources",
                                    after="upload_resources_data")
        self.machine.add_transition("end", "upload_resources", "end", after="end_process")

    def fetch_subscriptions_data(self):
        print("Fetching subscriptions...")
        self.subscriptions = self.subscription_client.fetch_subscriptions()

    def upload_subscriptions_data(self):
        print("Uploading subscriptions...")
        self.blob_client.upload_json(self.config["subscriptions_blob_name"], self.subscriptions)

    def fetch_resources_data(self):
        print("Fetching resources...")
        for subscription in self.subscriptions:
            subscription_id = subscription["id"]
            self.resources.extend(self.subscription_client.fetch_resources(subscription_id))

    def upload_resources_data(self):
        print("Uploading resources...")
        self.blob_client.upload_json(self.config["resources_blob_name"], self.resources)

    def end_process(self):
        print("Workflow completed successfully!")
