"""AWS credentials testing utility.

This script tests AWS credentials by attempting to list S3 buckets
and provides detailed error information if the credentials are invalid.
"""

import boto3
import os
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from pipeline.logging_config import create_logger

# Set up logging
logger = create_logger(__name__)

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), '.env')
logger.info(f"Loading .env file from: {dotenv_path}")
load_dotenv(dotenv_path, override=True)

def test_aws_credentials():
    """Test AWS credentials by attempting to list S3 buckets."""
    try:
        # Print the role ARN being used
        role_arn = os.getenv('AWS_ROLE_ARN')
        logger.info(f"Using role ARN: {role_arn}")
        
        # Create an S3 client
        s3 = boto3.client('s3')
        
        # Try to list buckets
        response = s3.list_buckets()
        
        # Log success
        bucket_count = len(response['Buckets'])
        logger.info(f"✅ AWS credentials are valid! You have access to {bucket_count} buckets.")
        
        # List the buckets
        if bucket_count > 0:
            logger.info("Accessible buckets:")
            for bucket in response['Buckets']:
                logger.info(f"  - {bucket['Name']}")
        
        return True
        
    except NoCredentialsError:
        logger.error("❌ No AWS credentials found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
        return False
    except ClientError as e:
        logger.error(f"❌ AWS credentials error: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    test_aws_credentials() 