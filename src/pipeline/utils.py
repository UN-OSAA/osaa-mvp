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
    Initialize S3 client using STS to assume a role.
    
    The function will authenticate using AWS access keys and assume an IAM role
    for enhanced security. All S3 operations are performed with the assumed role's permissions.

    :param return_session: If True, returns both client and session
    :return: S3 client, and optionally the session
    :raises ClientError: If S3 initialization fails
    """
    logger = create_logger(__name__)
    s3_client = None
    session = None
    
    # Check for bypass flag
    if os.getenv("SKIP_AWS_VALIDATION", "false").lower() == "true":
        logger.warning("⚠️ S3 CLIENT INITIALIZATION BYPASSED (TESTING MODE) ⚠️")
        logger.warning("Using mock S3 client for testing")
        # Return a simple mock object for testing
        class MockS3Client:
            def __init__(self):
                self.bucket_name = os.environ.get("S3_BUCKET_NAME", "test-bucket")
                
            def put_object(self, **kwargs):
                logger.info(f"MOCK: Would upload to {kwargs.get('Bucket')}/{kwargs.get('Key')}")
                return {"ETag": "mock-etag"}
                
            def get_object(self, **kwargs):
                logger.info(f"MOCK: Would download from {kwargs.get('Bucket')}/{kwargs.get('Key')}")
                return {"Body": io.BytesIO(b"mock data")}
                
            def list_objects_v2(self, **kwargs):
                logger.info(f"MOCK: Would list objects in {kwargs.get('Bucket')}/{kwargs.get('Prefix', '')}")
                return {"Contents": []}
                
        # Create mock session if needed
        if return_session:
            class MockSession:
                def __init__(self):
                    self.region_name = os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")
                    
                def client(self, service_name, **kwargs):
                    return MockS3Client()
                    
                def get_credentials(self):
                    class MockCredentials:
                        def __init__(self):
                            self.access_key = "mock-access-key"
                            self.secret_key = "mock-secret-key"
                            self.token = "mock-session-token"
                            
                        def get_frozen_credentials(self):
                            class FrozenCredentials:
                                def __init__(self):
                                    self.access_key = "mock-access-key"
                                    self.secret_key = "mock-secret-key"
                                    self.token = "mock-session-token"
                            return FrozenCredentials()
                    return MockCredentials()
                    
            mock_session = MockSession()
            return MockS3Client(), mock_session
        else:
            return MockS3Client()

    try:
        # Print all environment variables for AWS
        logger.info("==== AWS ENVIRONMENT VARIABLES DUMP ====")
        for key, value in os.environ.items():
            if key.startswith("AWS_"):
                masked_value = value
                if key in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]:
                    masked_value = f"{value[:4]}...{value[-4:]}" if len(value) > 8 else "***"
                logger.info(f"{key}: {masked_value}")
        logger.info("==== END ENVIRONMENT VARIABLES DUMP ====")
        
        # Get role ARN from environment
        role_arn = os.environ.get("AWS_ROLE_ARN")
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        session_token = os.environ.get("AWS_SESSION_TOKEN")  # May be None if not provided
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        
        # Add debug logging
        logger.info(f"AWS Region: {region}")
        logger.info(f"Access Key ID: {access_key[:4]}{'*' * (len(access_key) - 8)}{access_key[-4:] if len(access_key) > 8 else ''}")
        logger.info(f"Secret Access Key: {'*' * 8}{secret_key[-4:] if len(secret_key) > 4 else ''}")
        logger.info(f"Role ARN: {role_arn if role_arn else 'Not provided - using direct credentials'}")
        logger.info(f"Session Token provided: {bool(session_token)}")
        if session_token:
            logger.info(f"Session Token Length: {len(session_token)}")
            logger.info(f"Session Token first chars: {session_token[:10]}...")
            logger.info(f"Session Token last chars: ...{session_token[-10:]}")
        
        if access_key.startswith("ASIA"):
            logger.info("Detected temporary credentials (ASIA prefix)")
            if not session_token:
                logger.warning("⚠️ CRITICAL: Temporary credentials detected but no session token provided!")

        # If no role ARN is provided, use direct credentials
        if not role_arn:
            logger.info("No AWS_ROLE_ARN provided, using direct credentials for S3")
            
            # Create session with direct credentials
            session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name=region
            )
            
            # Create S3 client from session
            s3_client = session.client('s3')
            logger.info("Successfully created S3 client with direct credentials")
            
            if return_session:
                return s3_client, session
            else:
                return s3_client
                
        # Otherwise proceed with role assumption
        # Create STS client with session token if available
        sts_kwargs = {
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
        
        # Add session token if it exists (for temporary credentials)
        if session_token:
            sts_kwargs["aws_access_key_id"] = access_key
            sts_kwargs["aws_secret_access_key"] = secret_key
            sts_kwargs["aws_session_token"] = session_token
            logger.info("Using session token for AWS authentication")
        else:
            # If no session token, still add access and secret keys
            sts_kwargs["aws_access_key_id"] = access_key
            sts_kwargs["aws_secret_access_key"] = secret_key
            logger.info("Using standard AWS credentials without session token")

        # Create STS client
        logger.info("Creating STS client...")
        sts_client = boto3.client("sts", **sts_kwargs)

        # Assume role
        logger.info(f"Assuming role: {role_arn}")
        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn, RoleSessionName="OsaaMvpSession"
        )

        # Get temporary credentials
        credentials = assumed_role_object["Credentials"]

        # Create session with temporary credentials
        session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=region,
        )
            
        # Create S3 client from session
        logger.info("Creating S3 client from temporary session")
        s3_client = session.client("s3")
            
        # Test bucket listing
        try:
            s3_client.list_buckets()
            logger.info("S3 client initialized successfully with temporary credentials")
        except ClientError as access_error:
            error_code = access_error.response["Error"]["Code"]
            error_message = access_error.response["Error"]["Message"]
            logger.error(f"S3 Access Error: {error_code}")
            logger.error(f"Detailed Error Message: {error_message}")
            raise

        return (s3_client, session) if return_session else s3_client

    except Exception as e:
        logger.critical(f"Failed to initialize S3 client: {e}")
        log_aws_initialization_error(e)
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
