# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import json
from workflow.azure_workflow import AzureWorkflow

if __name__ == "__main__":
    # Load configuration
    # with open("config/settings.json") as f:
    #     config = json.load(f)

    # Initialize workflow
    workflow = AzureWorkflow()
    workflow.on_start()