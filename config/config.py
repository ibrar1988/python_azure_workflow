import json

# container_name_azure = "azure-greenfield"
# folder_raw_data = "raw_data"
# path_raw_data_subscription = f"{container_name_azure}/{folder_raw_data}/subscription"
# path_raw_data_resource = f"{container_name_azure}/{folder_raw_data}/resource"

class AzureConfig:
    def __init__(self):
        with open("config/settings.json") as f:
            self.config = json.load(f)

        # getter method
    def get_config(self):
        return self.config

