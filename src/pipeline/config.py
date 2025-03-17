"""Configuration module for project settings and environment variables.

This module manages configuration settings and environment-specific
parameters for the United Nations OSAA MVP project.
"""

import logging
import os
import sys

import boto3
import colorlog
from botocore.exceptions import ClientError

from pipeline.exceptions import ConfigurationError

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


# Logging configuration
def create_logger():
    """
    Create a structured, color-coded logger with clean output.

    :return: Configured logger instance
    """
    # Create logger
    logger = colorlog.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create console handler
    console_handler = colorlog.StreamHandler()

    # Custom log format with clear structure
    formatter = colorlog.ColoredFormatter(
        # Structured format with clear sections
        "%(log_color)s[%(levelname)s]%(reset)s %(blue)s[%(name)s]%(reset)s %(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
        secondary_log_colors={},
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


# Global logger instance
logger = create_logger()


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
    Validate AWS credentials by attempting to create a session
    and make a test call to AWS.
    """
    def _mask_sensitive(value):
        """Mask sensitive information in logs."""
        if not value:
            return None
        if len(value) <= 8:
            return "***"
        return f"{value[:4]}...{value[-4:]}"

    try:
        session = boto3.Session()
        credentials = session.get_credentials()
        if not credentials:
            raise ConfigurationError("No AWS credentials found")

        # Get frozen credentials to check expiration
        frozen_creds = credentials.get_frozen_credentials()
        
        # Check if using temporary credentials (ASIA prefix)
        if hasattr(frozen_creds, 'access_key') and frozen_creds.access_key.startswith('ASIA'):
            if not hasattr(frozen_creds, 'token'):
                raise ConfigurationError(
                    "Temporary credentials detected (ASIA) but no session token found. "
                    "Please ensure you have set AWS_SESSION_TOKEN in your environment."
                )
        
        # Test the credentials with a simple S3 operation
        try:
            s3 = session.client('s3')
            s3.list_buckets()
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            if error_code == 'ExpiredToken':
                raise ConfigurationError(
                    "\n🔑 AWS Token Expired! 🔑\n"
                    "Your temporary AWS credentials have expired. To refresh them:\n"
                    "1. Run 'aws sso login' if using SSO\n"
                    "2. Or get new temporary credentials from your AWS Console\n"
                    "3. Update your environment variables with the new credentials\n"
                    "\nNote: Temporary credentials typically expire after 1 hour.\n"
                    f"Error details: {error_message}"
                )
            elif error_code == 'InvalidAccessKeyId':
                raise ConfigurationError(
                    "\n❌ Invalid AWS Access Key ❌\n"
                    "Your AWS access key is not valid. Please check:\n"
                    "1. AWS_ACCESS_KEY_ID is set correctly\n"
                    "2. You are using the correct credentials for this environment\n"
                    f"\nError details: {error_message}"
                )
            elif error_code == 'SignatureDoesNotMatch':
                raise ConfigurationError(
                    "\n❌ Invalid AWS Secret Key ❌\n"
                    "Your AWS secret key is not valid. Please check:\n"
                    "1. AWS_SECRET_ACCESS_KEY is set correctly\n"
                    "2. The secret key matches your access key\n"
                    f"\nError details: {error_message}"
                )
            else:
                raise ConfigurationError(f"AWS Authentication Error: {error_message}")
            
    except Exception as e:
        if isinstance(e, ConfigurationError):
            raise
        raise ConfigurationError(f"AWS credentials validation failed: {str(e)}")


# Validate configuration and AWS credentials when module is imported
try:
    validate_config()
    if os.getenv("SKIP_AWS_VALIDATION", "false").lower() != "true":
        validate_aws_credentials()
except ConfigurationError as config_error:
    sys.exit(1)
