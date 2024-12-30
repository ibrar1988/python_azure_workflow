import json

class AzureConfig:
    def __init__(self):
        with open("config/settings.json") as f:
            self.config = json.load(f)

        # getter method
    def get_config(self):
        return self.config

