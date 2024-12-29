from datetime import datetime
import json


def write_json_data(file_prefix, new_data):
    current_date_timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
    file_name = f"{file_prefix}_{current_date_timestamp}.json"
    data = new_data
    with open(file_name, 'w') as file:
        json.dump(data, file, indent=4)
    return file_name


def generate_filename(file_prefix: str, page_number: int = None) -> str:
    """
        Generate a filename with an optional page number.

        :param file_prefix: Prefix for the filename.
        :param page_number: Optional page number to include in the filename.
        :return: Generated filename string.
        """
    current_date_timestamp = datetime.now().strftime('%Y_%m_%d')
    if page_number is not None:
        file_name = f"{file_prefix}_{current_date_timestamp}_page_{page_number}.json"
    else:
        file_name = f"{file_prefix}_{current_date_timestamp}.json"
    return file_name

def get_subscription_path_container_name(root_folder_name:str, container_name):
    path_raw_data_subscription = f"{container_name}/{root_folder_name}/subscription"
    return path_raw_data_subscription

def get_resource_path_container_name(root_folder_name:str, container_name):
    return f"{container_name}/{root_folder_name}/resource"
