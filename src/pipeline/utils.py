"""Utility module for common functions and helpers.

This module provides utility functions, logging setup, and helper
methods used across the United Nations OSAA MVP project.
"""

import logging
import os
import re
from typing import Dict, Optional, Tuple, Union, cast

import boto3

import pipeline.config as config


# LOGGER
def setup_logger(
    script_name: str, log_level: Union[str, int] = logging.INFO, log_file: Optional[str] = None
) -> logging.Logger:
    """Configure and return a logger for the specified script.

    Args:
        script_name: Name of the script for logging identification
        log_level: Logging level (default: logging.INFO)
        log_file: Optional file path to save log output

    Returns:
        Configured logging.Logger instance
    """
    logger = logging.getLogger(script_name)
    logger.setLevel(log_level)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    # Create file handler if log_file is provided
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        logger.addHandler(file_handler)

    # Create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


logger = setup_logger(__name__)


# S3 INITIALIZER
def s3_init(
    return_session: bool = False,
) -> Union[boto3.client, Tuple[boto3.client, boto3.Session]]:
    """Initialize and return an S3 client using credentials from environment variables.

    Args:
        return_session: If True, returns both S3 client and session

    Returns:
        Boto3 S3 client object, or tuple of (S3 client, session) if return_session is True
    """
    logger.info("🌐 Initializing S3 Client")

    if not config.ENABLE_S3_UPLOAD:
        logger.warning("   ⚠️ S3 upload is disabled")
        return (None, None) if return_session else None

    from dotenv import load_dotenv

    load_dotenv()

    try:
        # Try environment variables first (makes logic compatible with local runs and docker)
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.getenv(
            "AWS_SECRET_ACCESS_KEY"
        )
        aws_region = os.environ.get("AWS_DEFAULT_REGION") or os.getenv("AWS_DEFAULT_REGION")

        if not all([aws_access_key, aws_secret_key, aws_region]):
            logger.error("   ❌ Missing required AWS credentials")
            raise ValueError("Missing required AWS credentials")

        logger.info(f"   🌍 AWS Region: {aws_region}")
        logger.info("   🔑 Initializing AWS Session")

        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region,
        )

        s3_client = session.client("s3")

        logger.info("   ✅ S3 client initialized successfully")

        if return_session:
            return s3_client, session
        else:
            return s3_client

    except Exception as e:
        logger.error(f"   ❌ Error initializing S3 client: {e}")
        raise


# AWS S3 INTERACTIONS
def get_s3_file_paths(bucket_name: str, prefix: str) -> Dict[str, Dict[str, str]]:
    """Get a list of file paths from the S3 bucket and organize them into a dictionary.

    Args:
        bucket_name: The name of the S3 bucket.
        prefix: The folder prefix to filter the file paths.

    Returns:
        Dictionary of file paths organized by source folder.
    """
    try:
        logger.info(f"🔍 Retrieving file paths from S3 bucket: {bucket_name}")
        logger.info(f"   📂 Prefix: {prefix}")

        s3_prefix = prefix + "/"

        # Ensure s3_client is not None before calling get_paginator
        s3_client_result = s3_init()
        if s3_client_result is None:
            logger.error("   ❌ S3 client initialization failed")
            raise ValueError("S3 client initialization failed")

        # Cast to boto3.client to resolve type checking issues
        s3_client = cast(boto3.client, s3_client_result)
        paginator = s3_client.get_paginator("list_objects_v2")
        operation_parameters = {"Bucket": bucket_name, "Prefix": s3_prefix}
        page_iterator = paginator.paginate(**operation_parameters)
        filtered_iterator = page_iterator.search(f"Contents[?Key != '{s3_prefix}'][]")

        file_paths: Dict[str, Dict[str, str]] = {}
        for key_data in filtered_iterator:
            key = key_data["Key"]
            parts = key.split("/")
            if len(parts) >= 4:  # Ensure we have [env/landing/source/filename]
                source, filename = parts[-2], parts[-1]  # Take the last two parts
                if source not in file_paths:
                    file_paths[source] = {}
                file_paths[source][filename.split(".")[0]] = f"s3://{bucket_name}/{key}"

        logger.info(
            "   ✅ Retrieved {} file paths".format(
                sum(len(source_files) for source_files in file_paths.values())
            )
        )
        return file_paths
    except Exception as e:
        logger.error(f"   ❌ Error retrieving file paths from S3: {e}")
        raise


def download_s3_client(
    s3_client: boto3.client, s3_bucket_name: str, s3_folder: str, local_dir: str
) -> None:
    """Download all files from a specified S3 folder to a local directory.

    Args:
        s3_client: The boto3 S3 client.
        s3_bucket_name: The name of the S3 bucket.
        s3_folder: The folder within the S3 bucket to download files from.
        local_dir: The local directory to save downloaded files.
    """
    try:
        logger.info(f"📥 Downloading files from S3 bucket: {s3_bucket_name}")
        logger.info(f"   📂 Source Folder: {s3_folder}")
        logger.info(f"   💾 Local Destination: {local_dir}")

        # Ensure the local directory exists
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
            logger.info(f"   🆕 Created local directory: {local_dir}")

        # List objects in the specified S3 folder
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_folder)

        # Check if any objects are returned
        if "Contents" in response:
            file_count = 0
            for obj in response["Contents"]:
                s3_key = obj["Key"]
                filename = s3_key.split("/")[-1]  # Take only the last part as filename
                local_file_path = os.path.join(local_dir, filename)

                # Download the file
                s3_client.download_file(s3_bucket_name, s3_key, local_file_path)
                logger.info(f"   ✅ Downloaded {s3_key} to {local_file_path}")
                file_count += 1

            logger.info(f"📊 Successfully downloaded {file_count} files")
        else:
            logger.warning(f"   ⚠️ No files found in s3://{s3_bucket_name}/{s3_folder}")

    except Exception as e:
        logger.error(f"   ❌ Error downloading files from S3: {e}", exc_info=True)
        raise


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
