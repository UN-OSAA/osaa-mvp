import os
import logging
import sys
import time
import functools
from typing import Callable, Any, Tuple, Optional

import boto3
from botocore.exceptions import ClientError
import colorlog
import pipeline.config as config

### LOGGER ###
def setup_logger(name: str, log_dir: str = 'logs', log_level: int = logging.INFO):
    """
    Create a logger with robust configuration and color support.
    
    :param name: Name of the logger
    :param log_dir: Directory to store log files
    :param log_level: Logging level
    :return: Configured logger
    """
    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Clear existing handlers to prevent duplicate logs
    logger.handlers.clear()
    
    # Console Handler with Color
    console_handler = colorlog.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    
    # Colored Formatter
    console_formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white'
        },
        secondary_log_colors={},
        style='%'
    )
    console_handler.setFormatter(console_formatter)
    
    # File Handler (non-colored)
    log_file_path = os.path.join(log_dir, f'{name}.log')
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.DEBUG)
    
    # File Formatter
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0, 
          exceptions: Tuple[type, ...] = (Exception,)) -> Callable:
    """
    Retry decorator with exponential backoff.
    
    :param max_attempts: Maximum number of retry attempts
    :param delay: Initial delay between retries
    :param backoff: Multiplier for delay between retries
    :param exceptions: Tuple of exceptions to catch and retry
    :return: Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            current_delay = delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logger.warning(f"Attempt {attempt} failed: {e}")
                    
                    if attempt == max_attempts:
                        logger.error(f"All {max_attempts} attempts failed")
                        raise
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator

class ProcessMonitor:
    """
    A utility class for monitoring process performance and tracking errors.
    """
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize ProcessMonitor.
        
        :param logger: Optional logger. If not provided, creates a default logger.
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def track_processing_time(self, func: Callable) -> Callable:
        """
        Decorator to track processing time of a function.
        
        :param func: Function to monitor
        :return: Wrapped function with processing time tracking
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                end_time = time.time()
                
                # Log processing time
                self.logger.info(
                    f"Function {func.__name__} completed. "
                    f"Processing time: {end_time - start_time:.2f} seconds"
                )
                
                return result
            except Exception as e:
                end_time = time.time()
                self.logger.error(
                    f"Function {func.__name__} failed. "
                    f"Processing time: {end_time - start_time:.2f} seconds. "
                    f"Error: {e}"
                )
                raise
        return wrapper
    
    def send_error_alert(self, error: Exception, context: Optional[dict] = None):
        """
        Send an alert for critical errors.
        This is a placeholder - you'd replace with actual alerting mechanism
        (e.g., email, Slack, PagerDuty)
        
        :param error: Exception that occurred
        :param context: Additional context about the error
        """
        alert_message = f"Critical Error: {type(error).__name__}\n"
        alert_message += f"Message: {str(error)}\n"
        
        if context:
            alert_message += "Context:\n"
            for key, value in context.items():
                alert_message += f"  {key}: {value}\n"
        
        self.logger.critical(alert_message)
        # TODO: Implement actual alerting mechanism (e.g., email, Slack)

def s3_init(return_session: bool = False) -> Tuple[Any, Optional[Any]]:
    """
    Initialize S3 client with error handling and optional session return.
    
    :param return_session: If True, returns both client and session
    :return: S3 client, and optionally the session
    :raises ClientError: If S3 initialization fails
    """
    try:
        # Create a session
        session = boto3.Session()
        
        # Create S3 client
        s3_client = session.client('s3')
        
        # Optional logging of S3 client initialization
        logger = logging.getLogger(__name__)
        logger.info("S3 client initialized successfully")
        
        return (s3_client, session) if return_session else s3_client
    
    except ClientError as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to initialize S3 client: {e}")
        raise

### AWS S3 INTERACTIONS ###
def get_s3_file_paths(bucket_name: str, prefix: str) -> dict:
    """
    Get a list of file paths from the S3 bucket and organize them into a dictionary.

    :param bucket_name: The name of the S3 bucket.
    :param prefix: The folder prefix to filter the file paths.
    :return: Dictionary of file paths organized by source folder.
    """
    try:
        s3_prefix = prefix + '/'

        paginator = s3_init().get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': bucket_name, 'Prefix': s3_prefix}
        page_iterator = paginator.paginate(**operation_parameters)
        filtered_iterator = page_iterator.search(f"Contents[?Key != '{s3_prefix}'][]")

        file_paths = {}
        for key_data in filtered_iterator:
            key = key_data['Key']
            parts = key.split('/')
            if len(parts) >= 4:  # Ensure we have [env/landing/source/filename]
                source, filename = parts[-2], parts[-1]  # Take the last two parts
                if source not in file_paths:
                    file_paths[source] = {}
                file_paths[source][filename.split('.')[0]] = f"s3://{bucket_name}/{key}"

        logger = logging.getLogger(__name__)
        logger.info(f"Successfully retrieved file paths from S3 bucket {bucket_name}.")
        return file_paths
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error retrieving file paths from S3: {e}")
        raise

@retry(max_attempts=3, delay=1.0, backoff=2.0, exceptions=(Exception,))
def download_s3_client(s3_client: boto3.client, s3_bucket_name: str, s3_folder: str, local_dir: str) -> None:
    """
    Download all files from a specified S3 folder to a local directory.

    :param s3_client: The boto3 S3 client.
    :param s3_bucket_name: The name of the S3 bucket.
    :param s3_folder: The folder within the S3 bucket to download files from.
    :param local_dir: The local directory to save downloaded files.
    """
    try:
        # Ensure the local directory exists
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # List objects in the specified S3 folder
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_folder)
        
        # Check if any objects are returned
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_key = obj['Key']
                filename = s3_key.split('/')[-1]  # Take only the last part as filename
                local_file_path = os.path.join(local_dir, filename)
                
                # Download the file
                s3_client.download_file(s3_bucket_name, s3_key, local_file_path)
                logger = logging.getLogger(__name__)
                logger.info(f'Successfully downloaded {s3_key} to {local_file_path}')
        else:
            logger = logging.getLogger(__name__)
            logger.warning(f'No files found in s3://{s3_bucket_name}/{s3_folder}')

    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f'Error downloading files from S3: {e}', exc_info=True)
        raise