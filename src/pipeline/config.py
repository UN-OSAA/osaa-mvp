import os
import boto3
from pipeline.exceptions import ConfigurationError

# get the local root directory 
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))



# Define the LOCAL DATA directory relative to the root
    # RAW_DATA_DIR = os.path.join(ROOT_DIR, 'raw_data')
    # PROC_DATA_DIR = os.path.join(ROOT_DIR, 'processed')

DATALAKE_DIR = os.path.join(ROOT_DIR, 'data')
RAW_DATA_DIR = os.getenv('RAW_DATA_DIR', os.path.join(DATALAKE_DIR, 'raw'))
STAGING_DATA_DIR = os.path.join(DATALAKE_DIR, 'staging')
MASTER_DATA_DIR = os.path.join(STAGING_DATA_DIR, 'master')

# Allow both Docker and local environment DuckDB path
DB_PATH = os.getenv('DB_PATH', os.path.join(ROOT_DIR, 'sqlMesh', 'osaa_mvp.db'))

# Environment configurations
TARGET = os.getenv('TARGET', 'dev').lower()
USERNAME = os.getenv('USERNAME', 'default').lower()

S3_ENV = TARGET if TARGET in ['prod', 'int'] else f"{TARGET}_{USERNAME}"

ENABLE_S3_UPLOAD = os.getenv('ENABLE_S3_UPLOAD', 'true').lower() == 'true'

# S3 configurations with environment-based paths
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'osaa-mvp')
LANDING_AREA_FOLDER = f'{S3_ENV}/landing'
TRANSFORMED_AREA_FOLDER = f'{S3_ENV}/transformed'
STAGING_AREA_PATH = f'{S3_ENV}/staging'

# Local copy of master data
LOCAL=True

# Custom Exception for Configuration Errors
class ConfigurationError(Exception):
    """Exception raised for configuration-related errors."""
    pass

def validate_config():
    """
    Validate critical configuration parameters.
    Raises ConfigurationError if any required config is missing or invalid.
    
    :raises ConfigurationError: If configuration is invalid
    """
    # Validate root directories
    required_dirs = [
        ('ROOT_DIR', ROOT_DIR),
        ('DATALAKE_DIR', DATALAKE_DIR),
        ('RAW_DATA_DIR', RAW_DATA_DIR),
        ('STAGING_DATA_DIR', STAGING_DATA_DIR),
        ('MASTER_DATA_DIR', MASTER_DATA_DIR)
    ]
    
    for dir_name, dir_path in required_dirs:
        if not dir_path:
            raise ConfigurationError(f"Missing required directory configuration: {dir_name}")
        
        # Create directory if it doesn't exist
        try:
            os.makedirs(dir_path, exist_ok=True)
        except Exception as e:
            raise ConfigurationError(f"Unable to create directory {dir_name} at {dir_path}: {e}")
    
    # Validate DB Path
    if not DB_PATH:
        raise ConfigurationError("Database path (DB_PATH) is not configured")
    
    try:
        # Ensure DB directory exists
        db_dir = os.path.dirname(DB_PATH)
        os.makedirs(db_dir, exist_ok=True)
    except Exception as e:
        raise ConfigurationError(f"Unable to create database directory at {db_dir}: {e}")
    
    # Validate S3 Configuration
    if ENABLE_S3_UPLOAD:
        if not S3_BUCKET_NAME:
            raise ConfigurationError("S3 upload is enabled but no bucket name is specified")
        
        # Validate S3 folder configurations
        s3_folders = [
            ('LANDING_AREA_FOLDER', LANDING_AREA_FOLDER),
            ('TRANSFORMED_AREA_FOLDER', TRANSFORMED_AREA_FOLDER),
            ('STAGING_AREA_PATH', STAGING_AREA_PATH)
        ]
        
        for folder_name, folder_path in s3_folders:
            if not folder_path:
                raise ConfigurationError(f"Missing S3 folder configuration: {folder_name}")
    
    # Validate environment configurations
    if not TARGET:
        raise ConfigurationError("TARGET environment is not set")
    
    # Log validation success (optional)
    print("Configuration validation successful")

def validate_aws_credentials():
    """
    Validate AWS credentials before attempting S3 operations.
    
    :raises ConfigurationError: If AWS credentials are invalid or missing
    """
    try:
        required_vars = [
            'AWS_ACCESS_KEY_ID', 
            'AWS_SECRET_ACCESS_KEY', 
            'AWS_DEFAULT_REGION'
        ]
        
        for var in required_vars:
            if not os.getenv(var):
                raise ConfigurationError(f"Missing AWS credential: {var}")
        
        # Optional lightweight test
        s3_client = boto3.client('s3')
        s3_client.list_buckets()  # Lightweight credentials check
        
    except Exception as e:
        raise ConfigurationError(f"AWS Credentials Validation Failed: {e}")

# Validate configuration and AWS credentials when module is imported
validate_config()
validate_aws_credentials()
