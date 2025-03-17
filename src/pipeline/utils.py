import functools
import os
import re
import time
import io
from typing import Any, Callable, Dict, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
import logging

from pipeline.logging_config import create_logger, log_exception


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[type, ...] = (Exception,),
) -> Callable:
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
            logger = create_logger(func.__module__)
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


def log_aws_initialization_error(error):
    """
    Comprehensive logging for AWS S3 initialization errors.

    :param error: The exception raised during AWS S3 initialization
    """
    logger = create_logger(__name__)

    # Comprehensive error logging
    logger.critical(f"AWS S3 Initialization Failed: {error}")
    logger.critical("Troubleshooting:")
    logger.critical("1. Verify AWS credentials")
    logger.critical("2. Check IAM user permissions")
    logger.critical("3. Ensure AWS IAM user has S3 access")


def s3_init(return_session: bool = False) -> Tuple[Any, Optional[Any]]:
    """
    Initialize S3 client and optionally return the session.
    Returns a tuple of (s3_client, session) where session is None if return_session is False.
    """
    logger = create_logger(__name__)
    try:
        session = boto3.Session()
        s3_client = session.client('s3')
        
        # Test connection with a simple operation
        try:
            s3_client.list_buckets()
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            if error_code == 'ExpiredToken':
                logger.error(
                    "\n🔑 AWS Token Expired! 🔑\n"
                    "Your temporary AWS credentials have expired. To refresh them:\n"
                    "1. Run 'aws sso login' if using SSO\n"
                    "2. Or get new temporary credentials from your AWS Console\n"
                    "3. Update your environment variables with the new credentials\n"
                    "\nNote: Temporary credentials typically expire after 1 hour."
                )
                raise ConfigurationError(f"Token expired: {error_message}")
            else:
                raise ConfigurationError(f"S3 client initialization failed: {error_message}")
                
        return (s3_client, session if return_session else None)
        
    except Exception as e:
        if isinstance(e, ConfigurationError):
            raise
        logger.error(f"Failed to initialize S3 client: {str(e)}")
        raise ConfigurationError(f"S3 client initialization failed: {str(e)}")


# File path and naming utilities
def get_filename_from_path(file_path: str) -> str:
    """Extract filename from a given file path.

    Args:
        file_path: Full path to the file

    Returns:
        Extracted filename without extension
    """
    return os.path.splitext(os.path.basename(file_path))[0]


def standardize_filename(filename: str) -> str:
    """Standardize filename by removing special characters.

    Args:
        filename: Input filename to standardize

    Returns:
        Standardized filename with only alphanumeric characters and underscores
    """
    return re.sub(r"[^a-zA-Z0-9_]", "_", filename).lower()


def collect_file_paths(directory: str, file_extension: str) -> Dict[str, str]:
    """Collect file paths for a specific file extension in a directory.

    Args:
        directory: Directory to search for files
        file_extension: File extension to filter (e.g., '.csv')

    Returns:
        Dictionary mapping standardized filenames to full file paths
    """
    file_paths: Dict[str, str] = {}
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                full_path = os.path.join(root, file)
                filename = get_filename_from_path(file)
                std_filename = standardize_filename(filename)
                file_paths[std_filename] = full_path
    return file_paths
