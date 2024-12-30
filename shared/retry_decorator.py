from utils.logger_setup import setup_logger
from azure.core.exceptions import HttpResponseError
import functools
from time import sleep

logger = setup_logger(name="retry_with_backoff")

def retry_with_backoff(retries=3, backoff_in_seconds=1):
    def decorator(func):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            retry_count = 0
            wait_time = backoff_in_seconds

            while retry_count < retries:
                try:
                    return func(*args, **kwargs)
                except HttpResponseError as e:
                    if e.status_code == 429:
                        # Extract 'Retry-After' from headers, if available
                        if e.response and hasattr(e.response, "headers"):
                            retry_after = e.response.headers.get("Retry-After")
                        else:
                            retry_after = None
                        if retry_after:
                            wait_time = int(retry_after)
                            logger.warning(
                                f"Received 429 Too Many Requests. Retrying after {wait_time} seconds..."
                            )
                        else:
                            logger.warning(
                                f"Received 429 Too Many Requests but no 'Retry-After' header. "
                                f"Using default backoff time of {wait_time} seconds."
                            )
                    else:
                        logger.warning(
                            f"Attempt {retry_count + 1} failed for {func.__name__}: {e.message}. "
                            f"Retrying in {wait_time} seconds..."
                        )

                    retry_count += 1
                    if retry_count == retries:
                        logger.error(f"All {retries} attempts failed for {func.__name__}. Final error: {str(e)}")
                        raise

                    sleep(wait_time)
                    wait_time *= 2  # Exponential backoff for non-429 errors

                except Exception as e:
                    retry_count += 1
                    if retry_count == retries:
                        logger.error(f"All {retries} attempts failed for {func.__name__}. Final error: {str(e)}")
                        raise

                    logger.warning(
                        f"Attempt {retry_count} failed for {func.__name__}: {str(e)}. "
                        f"Retrying in {wait_time} seconds..."
                    )
                    sleep(wait_time)
                    wait_time *= 2  # Exponential backoff for unexpected exceptions

        return sync_wrapper

    return decorator
