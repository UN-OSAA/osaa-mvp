"""Test module for configuration settings and utilities.

This module contains unit tests for configuration-related
functions and utilities in the United Nations OSAA MVP project.
"""

import logging
import os

import pipeline.config as config

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def test_configuration() -> None:
    """Comprehensive configuration test to validate settings."""
    logger.info("🔍 Starting Configuration Validation")

    # Test Directories
    logger.info("📂 Validating Directories")
    directories_to_check = [
        ("Root Directory", config.ROOT_DIR),
        ("Datalake Directory", config.DATALAKE_DIR),
        ("Raw Data Directory", config.RAW_DATA_DIR),
        ("Staging Data Directory", config.STAGING_DATA_DIR),
        ("Master Data Directory", config.MASTER_DATA_DIR),
    ]

    for name, directory in directories_to_check:
        if os.path.exists(directory):
            logger.info(f"   ✅ {name}: {directory} exists")
        else:
            logger.warning(f"   ❌ {name}: {directory} does NOT exist")

    # Test Environment Variables
    logger.info("🌐 Validating Environment Settings")
    env_checks = [
        ("Target Environment", config.TARGET, ["dev", "prod", "int"]),
        ("Username", config.USERNAME, None),
        ("S3 Environment", config.S3_ENV, None),
    ]

    for name, value, valid_options in env_checks:
        if valid_options and value not in valid_options:
            logger.warning(f"   ⚠️ {name}: {value} is not in expected options {valid_options}")
        else:
            logger.info(f"   ✅ {name}: {value}")

    # Test S3 Configuration
    logger.info("☁️ Validating S3 Configuration")
    s3_checks = [
        ("S3 Bucket Name", config.S3_BUCKET_NAME),
        ("Landing Area Folder", config.LANDING_AREA_FOLDER),
        ("Transformed Area Folder", config.TRANSFORMED_AREA_FOLDER),
        ("Staging Area Path", config.STAGING_AREA_PATH),
    ]

    for name, value in s3_checks:
        if value:
            logger.info(f"   ✅ {name}: {value}")
        else:
            logger.warning(f"   ❌ {name}: No value configured")

    # Test Database Configuration
    logger.info("🗃️ Validating Database Configuration")
    if os.path.exists(os.path.dirname(config.DB_PATH)):
        logger.info(f"   ✅ Database Path: {config.DB_PATH}")
    else:
        logger.warning(
            f"   ❌ Database Path Directory: {os.path.dirname(config.DB_PATH)} does NOT exist"
        )

    logger.info("✨ Configuration Validation Complete")


# Run the configuration test when the script is executed
if __name__ == "__main__":
    test_configuration()
