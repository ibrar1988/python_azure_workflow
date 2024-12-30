import time
from typing import Any

from azure.core.exceptions import AzureError
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import SubscriptionClient
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryResponse

from shared.retry_decorator import retry_with_backoff
from utils.logger_setup import setup_logger

logger = setup_logger(name="AzureSubscriptionClient")

class AzureSubscriptionClient:
    credential = None
    def __init__(self):
        self.credential = DefaultAzureCredential()
        self.subscription_client = SubscriptionClient(self.credential)
        logger.info("Initializing SubscriptionClient.")
        self.resource_client = None

    @retry_with_backoff(retries=3, backoff_in_seconds=1)
    def fetch_subscriptions(self) -> list[Any]:
        try:
            logger.info("Fetching subscription list.")
            response = self.subscription_client.subscriptions.list()
            subscriptions_data = [sub.as_dict() for sub in response]
            logger.info("Successfully retrieved Azure subscriptions.")
            return subscriptions_data
        except AzureError as e:
            logger.error(f"AzureError encountered: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred: {str(e)}")
            raise

    def get_resources_for_subscription_paginated(self, subscription_id, records_per_page, max_retries=3, retry_delay=5):
        if not subscription_id:
            raise ValueError("Subscription ID is required but was not provided.")
        try:
            resource_graph_client = ResourceGraphClient(self.credential)
            if records_per_page is None:
                records_per_page = 100
            logger.info(f"<<<< Per page : {records_per_page} with subscription id: {subscription_id}")
            query = QueryRequest(
                subscriptions=[subscription_id],
                query="""
                resources
                """,
                options={"resultFormat": "objectArray", "$top": records_per_page}  # Set records per page
            )


            # query1 = "resourcechanges | where todatetime(properties.changeAttributes.timestamp) > ago(24h)"
            #
            #
            # # Define the query request parameters
            # query_request = QueryRequest(
            #     subscriptions=[subscription_id],
            #     query=query1
            # )
            # # Run the query
            # response = resource_graph_client.resources(query_request)



            # Initial query
            result: QueryResponse = resource_graph_client.resources(query)
            if hasattr(result, 'data') and isinstance(result.data, list):
                yield result.data  # Yield the first page of data

            # Handle pagination with skipToken
            while result.skip_token is not None:
                # Create a new QueryRequest with updated options
                re_query = QueryRequest(
                    subscriptions=[subscription_id],
                    query="""
                    resources
                    """,
                    options={
                        "resultFormat": "objectArray",
                        "$skipToken": result.skip_token  # Add skipToken here
                    }
                )
                retries = 0
                while retries < max_retries:
                    try:
                        result = resource_graph_client.resources(re_query)
                        if hasattr(result, 'data') and isinstance(result.data, list):
                            yield result.data  # Yield each subsequent page
                        break  # Exit retry loop on success
                    except AzureError as e:
                        retries += 1
                        logger.warning(
                            f"Retry {retries}/{max_retries} for subscription {subscription_id} due to Azure error: {str(e)}")
                        time.sleep(retry_delay * retries)  # Exponential backoff
                    except Exception as e:
                        retries += 1
                        logger.warning(
                            f"Retry {retries}/{max_retries} for subscription {subscription_id} due to unexpected error: {str(e)}")
                        time.sleep(retry_delay * retries)

                if retries == max_retries:
                    logger.error(
                        f"Failed to fetch page after {max_retries} retries for subscription {subscription_id}. Skipping remaining pages.")
                    break  # Skip to the next subscription if retries are exhausted

        except AzureError as e:
            logger.error(f"Azure error occurred while fetching resources for subscription {subscription_id}: {str(e)}")
        except Exception as e:
            logger.error(
                f"Unexpected error occurred while fetching resources for subscription {subscription_id}: {str(e)}")
