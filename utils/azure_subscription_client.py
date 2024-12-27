from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import SubscriptionClient, ResourceManagementClient

class AzureSubscriptionClient:
    def __init__(self):
        self.credential = DefaultAzureCredential()
        self.subscription_client = SubscriptionClient(self.credential)
        self.resource_client = None

    def fetch_subscriptions(self):
        subscriptions = []
        for subscription in self.subscription_client.subscriptions.list():
            subscriptions.append({"id": subscription.subscription_id, "name": subscription.display_name})
        return subscriptions

    def fetch_resources(self, subscription_id):
        self.resource_client = ResourceManagementClient(self.credential, subscription_id)
        resources = []
        for resource in self.resource_client.resources.list():
            resources.append({
                "id": resource.id,
                "name": resource.name,
                "type": resource.type,
                "location": resource.location
            })
        return resources
