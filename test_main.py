from azure.functions import HttpRequest
from io import BytesIO
import json

# Import your main function
from main import main
from utils.logger_setup import setup_logger

logger = setup_logger(name="TestMain")

# Create a mock HttpRequest
req = HttpRequest(
    method="GET",
    url="/api/test",
    body=json.dumps({"key": "value"}).encode("utf-8"),  # Replace with your payload
    headers={"Content-Type": "application/json"}
)

# Call the main function
response = main(req)

# Print the response
logger.info(f"Status Code: {response.status_code}")
logger.info(f"Response Body: {response.get_body().decode('utf-8')}")
