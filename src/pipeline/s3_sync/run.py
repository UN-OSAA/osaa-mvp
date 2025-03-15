"""Module for handling S3 synchronization of SQLMesh database files.

This module provides functionality to sync SQLMesh database files with S3,
including downloading existing DBs and uploading updated ones.
"""

import os
import sys
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from pipeline.exceptions import S3OperationError
from pipeline.logging_config import create_logger
from pipeline.config import S3_BUCKET_NAME
from pipeline.utils import s3_init

logger = create_logger(__name__)

def sync_db_with_s3(operation: str, db_path: str, bucket_name: str, s3_key: str) -> None:
    """
    Sync SQLMesh database with S3.

    Args:
        operation: Either "download" or "upload"
        db_path: Local path to the SQLMesh database file
        bucket_name: S3 bucket name
        s3_key: Key (path) in S3 bucket

    Raises:
        S3OperationError: If S3 operations fail
    """
    try:
        # Use s3_init to properly handle session token and role assumption
        s3_client = s3_init()
        
        if operation == "download":
            logger.info("Attempting to download DB from S3...")
            try:
                s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                # File exists, download it
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
                s3_client.download_file(bucket_name, s3_key, db_path)
                logger.info("Successfully downloaded existing DB from S3")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logger.info("No existing DB found in S3, skipping download...")
                else:
                    raise S3OperationError(f"Error checking S3 object: {str(e)}")
                    
        elif operation == "upload":
            # Only allow uploads in prod/qa environments
            if os.getenv('TARGET', '').lower() not in ['prod', 'qa']:
                logger.warning("Upload operation restricted to prod/qa targets only")
                return
                
            logger.info("Uploading DB to S3...")
            if os.path.exists(db_path):
                s3_client.upload_file(db_path, bucket_name, s3_key)
                logger.info("Successfully uploaded DB to S3")
            else:
                logger.warning(f"Local DB file not found at {db_path}, skipping upload")
                
    except Exception as e:
        error_msg = f"S3 {operation} operation failed: {str(e)}"
        logger.error(error_msg)
        raise S3OperationError(error_msg)

def get_db_paths(db_filename: Optional[str] = "unosaa_data_pipeline.db") -> tuple[str, str]:
    """
    Get the local and S3 paths for the database file.
    
    Args:
        db_filename: Name of the database file
        
    Returns:
        Tuple of (local_path, s3_key)
    """
    local_path = f"sqlMesh/{db_filename}"
    s3_key = db_filename
    return local_path, s3_key

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ["download", "upload"]:
        print("Usage: python -m pipeline.s3_sync.run [download|upload]")
        sys.exit(1)

    operation = sys.argv[1]
    local_path, s3_key = get_db_paths()
    sync_db_with_s3(operation, local_path, S3_BUCKET_NAME, s3_key) 