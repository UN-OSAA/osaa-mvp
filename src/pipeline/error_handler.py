import logging
import sys
from pipeline.exceptions import PipelineBaseError

def global_exception_handler(exc_type, exc_value, exc_traceback):
    """
    Global exception handler to log unhandled exceptions.
    
    :param exc_type: Exception type
    :param exc_value: Exception value
    :param exc_traceback: Traceback object
    """
    if issubclass(exc_type, PipelineBaseError):
        logging.error(
            "Unhandled Pipeline Error",
            exc_info=(exc_type, exc_value, exc_traceback)
        )
    else:
        # Default exception handling
        logging.__excepthook__(exc_type, exc_value, exc_traceback)

# Optional: Set the global exception handler
# Uncomment if you want to use this globally
# sys.excepthook = global_exception_handler

def setup_global_exception_handling():
    """
    Set up global exception handling for the pipeline.
    """
    sys.excepthook = global_exception_handler
    logging.info("Global exception handler installed.")
