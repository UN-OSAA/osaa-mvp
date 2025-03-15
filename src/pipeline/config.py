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
    Validate AWS credentials by attempting to create an S3 client
    and make a test call to AWS.
    """
    # Skip validation if in testing mode
    if os.getenv("SKIP_AWS_VALIDATION", "false").lower() == "true":
        logger.warning("⚠️ AWS CREDENTIAL VALIDATION BYPASSED (TESTING MODE) ⚠️")
        logger.warning("This should only be used for local testing")
        return

    def _mask_sensitive(value):
        """Mask sensitive information in logs."""
        if not value:
            return "NOT SET"
        return "*" * (len(value) - 4) + value[-4:] if len(value) > 4 else "*" * len(value)

    try:
        # Credential validation stages
        logger.info("Validating AWS Credentials")

        # Check required environment variables
        required_vars = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_DEFAULT_REGION",
            "AWS_ROLE_ARN"
        ]
        
        # Log environment variable status
        logger.debug("Checking Environment Variables:")
        for var in required_vars:
            value = os.getenv(var)
            logger.debug(f"  {var}: {_mask_sensitive(value)}")

        # Validate variable presence
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            missing_list = ", ".join(missing_vars)
            raise ConfigurationError(f"Missing AWS credentials: {missing_list}")

        # Extract credentials
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        role_arn = os.getenv("AWS_ROLE_ARN")
        session_token = os.getenv("AWS_SESSION_TOKEN")  # May be None if not provided
        
        # Add detailed debug logging
        logger.info(f"AWS Region: {region}")
        logger.info(f"Access Key ID: {access_key[:4]}{'*' * (len(access_key) - 8)}{access_key[-4:] if len(access_key) > 8 else ''}")
        logger.info(f"Secret Access Key: {'*' * min(8, len(secret_key))}{secret_key[-4:] if len(secret_key) > 4 else ''}")
        logger.info(f"Role ARN: {role_arn}")
        logger.info(f"Session Token provided: {bool(session_token)}")
        if session_token:
            logger.info(f"Session Token Length: {len(session_token)}")
            
        if access_key.startswith("ASIA"):
            logger.info("Detected temporary credentials (ASIA prefix)")
            if not session_token:
                logger.warning("⚠️ CRITICAL: Temporary credentials detected but no session token provided!")
                logger.warning("Temporary credentials (ASIA prefix) require a session token")

        # Validate credential format
        if not access_key.startswith("AKIA") and not access_key.startswith("ASIA"):
            logger.warning("Potential non-standard AWS Access Key ID format")

        if len(access_key) < 10 or len(secret_key) < 20:
            raise ConfigurationError("Incomplete or malformed AWS credentials")

        # S3 client creation and validation
        try:
            # Create STS client
            logger.info("Creating STS client with explicit configuration")
            sts_kwargs = {
                "aws_access_key_id": access_key,
                "aws_secret_access_key": secret_key,
                "region_name": region
            }
            
            # Fix region endpoint format if needed
            if not region:
                logger.warning("AWS_DEFAULT_REGION not set, defaulting to eu-west-1")
                region = "eu-west-1"
                sts_kwargs["region_name"] = region
            
            # Make sure region doesn't have incorrect format
            if '..' in region:
                logger.warning(f"Malformed region detected: {region}, fixing to eu-west-1")
                region = "eu-west-1"
                sts_kwargs["region_name"] = region
                
            # Add explicit endpoint URL to avoid potential DNS issues
            endpoint_url = f"https://sts.{region}.amazonaws.com"
            sts_kwargs["endpoint_url"] = endpoint_url
            logger.info(f"Using explicit endpoint URL: {endpoint_url}")
            
            # Add session token if it exists
            if session_token:
                sts_kwargs["aws_session_token"] = session_token
                logger.info("Using session token for AWS authentication")
            
            # Create STS client
            sts_client = boto3.client("sts", **sts_kwargs)
            
            # Assume role to verify credentials
            try:
                assumed_role = sts_client.assume_role(
                    RoleArn=role_arn,
                    RoleSessionName="ValidationSession"
                )
                
                # Create S3 client with assumed role
                s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=assumed_role["Credentials"]["AccessKeyId"],
                    aws_secret_access_key=assumed_role["Credentials"]["SecretAccessKey"],
                    aws_session_token=assumed_role["Credentials"]["SessionToken"],
                    region_name=region
                )
                
                # Test bucket listing
                s3_client.list_buckets()
                logger.info("AWS credentials validated successfully")
                
            except ClientError as assume_error:
                error_code = assume_error.response["Error"]["Code"]
                error_message = assume_error.response["Error"]["Message"]
                logger.error(f"Role Assumption Error: {error_code}")
                logger.error(f"Error Details: {error_message}")
                raise ConfigurationError(f"Failed to assume role: {error_message}")
        except Exception as client_error:
            logger.error(f"AWS Client Creation Failed: {client_error}")
            raise ConfigurationError(f"AWS Authentication Error: {client_error}")

    except Exception as e:
        # Structured error reporting
        logger.critical("🔒 AWS CREDENTIALS VALIDATION FAILED 🔒")
        logger.critical(f"Error Type: {type(e).__name__}")
        logger.critical(f"Error Details: {str(e)}")

        # Concise troubleshooting guide
        troubleshooting_steps = [
            "1. Verify AWS credentials in .env",
            "2. Check IAM user permissions to assume the role",
            "3. Ensure the role has proper S3 permissions",
            "4. Verify network connectivity to AWS"
        ]

        for step in troubleshooting_steps:
            logger.critical(step)

        raise ConfigurationError(f"AWS Credential Validation Failed: {e}")


# Validate configuration and AWS credentials when module is imported
try:
    validate_config()
    if os.getenv("SKIP_AWS_VALIDATION", "false").lower() != "true":
        validate_aws_credentials()
except ConfigurationError as config_error:
    sys.exit(1)
