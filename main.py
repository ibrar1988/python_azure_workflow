import azure.functions as func

from utils.logger_setup import setup_logger
from workflow.azure_workflow import AzureWorkflow

logger = setup_logger(name="main")

def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("Workflow completed successfully")
    workflow = AzureWorkflow()
    try:
        # Start the workflow
        workflow.on_start()
        logger.info("Workflow completed successfully")
        return func.HttpResponse("Workflow completed successfully.", status_code=200)
    except Exception as e:
        # Log the error
        logger.error(f"An error occurred: {str(e)}")
        # Return HTTP 500 with the error message
        return func.HttpResponse(f"Internal Server Error: {str(e)}", status_code=500)
