"""Configuration module for project settings and environment variables.

This module manages configuration settings and environment-specific
parameters for the United Nations OSAA MVP project.
"""

import os
import sys
import boto3
import colorlog
import logging
from botocore.exceptions import ClientError
from pipeline.exceptions import ConfigurationError
from pipeline.logging_config import create_logger

# get the local root directory
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Define the LOCAL DATA directory relative to the root
# RAW_DATA_DIR = os.path.join(ROOT_DIR, 'raw_data')
# PROC_DATA_DIR = os.path.join(ROOT_DIR, 'processed')

DATALAKE_DIR = os.path.join(ROOT_DIR, "data")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", os.path.join(DATALAKE_DIR, "raw"))
STAGING_DATA_DIR = os.path.join(DATALAKE_DIR, "staging")
MASTER_DATA_DIR = os.path.join(STAGING_DATA_DIR, "master")

# Allow both Docker and local environment DuckDB path
DB_PATH = os.getenv(
    "DB_PATH", os.path.join(ROOT_DIR, "sqlMesh", "unosaa_data_pipeline.db")
)

# Environment configurations
TARGET = os.getenv("TARGET", "dev").lower()
USERNAME = os.getenv("USERNAME", "default").lower()

# Construct S3 environment path
S3_ENV = TARGET if TARGET == "prod" else f"dev/{TARGET}_{USERNAME}"

ENABLE_S3_UPLOAD = os.getenv("ENABLE_S3_UPLOAD", "true").lower() == "true"

# S3 configurations with environment-based paths
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "unosaa-data-pipeline")
LANDING_AREA_FOLDER = f"{S3_ENV}/landing"
STAGING_AREA_FOLDER = f"{S3_ENV}/staging"


# Custom Exception for Configuration Errors
class ConfigurationError(Exception):
    """Exception raised for configuration-related errors."""

    pass


# Global logger instance
logger = create_logger(__name__)


def validate_config():
    """
    Validate critical configuration parameters.
    Raises ConfigurationError if any required config is missing or invalid.

    :raises ConfigurationError: If configuration is invalid
    """
    # Validate root directories
    required_dirs = [
        ("ROOT_DIR", ROOT_DIR),
        ("DATALAKE_DIR", DATALAKE_DIR),
        ("RAW_DATA_DIR", RAW_DATA_DIR),
        ("STAGING_DATA_DIR", STAGING_DATA_DIR),
        ("MASTER_DATA_DIR", MASTER_DATA_DIR),
    ]

    for dir_name, dir_path in required_dirs:
        if not dir_path:
            raise ConfigurationError(
                f"Missing required directory configuration: {dir_name}"
            )

        # Create directory if it doesn't exist
        try:
            os.makedirs(dir_path, exist_ok=True)
        except Exception as e:
            raise ConfigurationError(
                f"Unable to create directory {dir_name} at {dir_path}: {e}"
            )

    # Validate DB Path
    if not DB_PATH:
        raise ConfigurationError("Database path (DB_PATH) is not configured")

    try:
        # Ensure DB directory exists
        db_dir = os.path.dirname(DB_PATH)
        os.makedirs(db_dir, exist_ok=True)
    except Exception as e:
        raise ConfigurationError(
            f"Unable to create database directory at {db_dir}: {e}"
        )

    # Validate S3 Configuration
    if ENABLE_S3_UPLOAD:
        if not S3_BUCKET_NAME:
            raise ConfigurationError(
                "S3 upload is enabled but no bucket name is specified"
            )

        # Validate S3 folder configurations
        s3_folders = [
            ("LANDING_AREA_FOLDER", LANDING_AREA_FOLDER),
            ("STAGING_AREA_FOLDER", STAGING_AREA_FOLDER),
        ]

        for folder_name, folder_path in s3_folders:
            if not folder_path:
                raise ConfigurationError(
                    f"Missing S3 folder configuration: {folder_name}"
                )

    # Validate environment configurations
    if not TARGET:
        raise ConfigurationError("TARGET environment is not set")

    # Log validation success (optional)
    logger.info("Configuration validation successful")


def validate_aws_credentials():
    """
    Validate AWS credentials with structured error handling.

    Performs comprehensive checks:
    - Verifies presence of required environment variables
    - Validates AWS credential format
    - Attempts S3 client creation
    - Performs lightweight bucket listing test

    :raises ConfigurationError: If credentials are invalid or missing
    """

    def _mask_sensitive(value):
        """Mask sensitive information in logs."""
        return "*" * len(value) if value else "NOT SET"

    try:
        # Credential validation stages
        logger.info("Validating AWS Credentials")

        # Check required environment variables
        required_vars = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_DEFAULT_REGION",
        ]

        # Log environment variable status
        logger.debug("Checking Environment Variables:")
        for var in required_vars:
            value = os.getenv(var)
            logger.debug(f"  {var}: {_mask_sensitive(value)}")

        # Validate variable presence
        for var in required_vars:
            if not os.getenv(var):
                raise ConfigurationError(f"Missing AWS credential: {var}")

        # Extract credentials
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

        # Validate credential format
        if not access_key.startswith("AKIA"):
            logger.warning("Potential non-standard AWS Access Key ID format")

        if len(access_key) < 10 or len(secret_key) < 20:
            raise ConfigurationError("Incomplete or malformed AWS credentials")

        # S3 client creation and validation
        try:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
            )

            # Lightweight bucket listing test
            try:
                s3_client.list_buckets()
                logger.info("AWS credentials validated successfully")

            except ClientError as list_error:
                error_code = list_error.response["Error"]["Code"]
                error_message = list_error.response["Error"]["Message"]

                # Specific handling for invalid access key
                if error_code == "InvalidAccessKeyId":
                    detailed_error = "Invalid AWS Access Key"
                    logger.error(detailed_error)
                    logger.error(f"Error Details: {error_message}")
                    raise ConfigurationError(detailed_error) from list_error

                # Generic S3 access error
                logger.error(f"S3 Access Error: {error_message}")
                raise ConfigurationError(f"S3 Access Failed: {error_message}")

        except Exception as client_error:
            logger.error(f"S3 Client Creation Failed: {client_error}")
            raise ConfigurationError(f"S3 Client Setup Error: {client_error}")

    except Exception as e:
        # Structured error reporting
        logger.critical("🔒 AWS CREDENTIALS VALIDATION FAILED 🔒")
        logger.critical(f"Error Type: {type(e).__name__}")
        logger.critical(f"Error Details: {str(e)}")

        # Concise troubleshooting guide
        troubleshooting_steps = [
            "1. Verify AWS credentials in .env",
            "2. Check IAM user permissions",
            "3. Regenerate AWS access keys",
            "4. Confirm AWS account and region",
        ]

        logger.critical("Troubleshooting:")
        for step in troubleshooting_steps:
            logger.critical(f"  {step}")

        logger.critical("Contact AWS administrator for assistance.")

        raise


# Validate configuration and AWS credentials when module is imported
try:
    validate_config()
    validate_aws_credentials()
except ConfigurationError as config_error:
    sys.exit(1)
