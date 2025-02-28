"""
S3 helper functions for the pipeline package.

This module provides utilities for interacting with AWS S3.
"""

import os
from typing import Any, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from pipeline.logging_config import create_logger, log_exception

# Initialize logger
logger = create_logger(__name__)


def s3_init(return_session: bool = False) -> Tuple[Any, Optional[Any]]:
    """
    Initialize S3 client using STS to assume a role.

    :param return_session: If True, returns both client and session
    :return: S3 client, and optionally the session
    :raises ClientError: If S3 initialization fails
    """
    try:
        # Get role ARN from environment
        role_arn = os.environ.get("AWS_ROLE_ARN")
        if not role_arn:
            raise ValueError("AWS_ROLE_ARN is not set in environment variables")

        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

        # Create STS client
        sts_client = boto3.client("sts")

        # Assume role
        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName="AssumeRoleSession"
        )

        # Get credentials from assumed role
        credentials = assumed_role_object["Credentials"]

        # Create session with assumed role credentials
        session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=region
        )

        # Create S3 client
        s3_client = session.client("s3")
        logger.info(f"S3 client initialized with assumed role: {role_arn}")

        if return_session:
            return s3_client, session
        else:
            return s3_client, None
    
    except ClientError as e:
        log_exception(logger, e, "Failed to initialize S3 client")
        raise
    except Exception as e:
        log_exception(logger, e, "Unexpected error initializing S3 client")
        raise 