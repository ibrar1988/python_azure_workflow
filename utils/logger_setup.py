import logging
import os


def setup_logger(name=__name__, level=None):
    # Get the logging level, defaulting to INFO
    level = level or os.getenv("LOGGING_LEVEL", "INFO").upper()

    level = getattr(logging, level, logging.DEBUG)
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(level)
    stream_handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)

    return logger
