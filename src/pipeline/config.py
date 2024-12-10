"""Configuration module for project settings and environment variables.

This module manages configuration settings and environment-specific
parameters for the United Nations OSAA MVP project.
"""

import logging
import os

# Setup logging
logger = logging.getLogger(__name__)

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
DB_PATH = os.getenv("DB_PATH", os.path.join(ROOT_DIR, "sqlMesh", "osaa_mvp.db"))

# Environment configurations
TARGET = os.getenv("TARGET", "dev").lower()
USERNAME = os.getenv("USERNAME", "default").lower()

S3_ENV = TARGET if TARGET in ["prod", "int"] else f"{TARGET}_{USERNAME}"

ENABLE_S3_UPLOAD = os.getenv("ENABLE_S3_UPLOAD", "true").lower() == "true"

# S3 configurations with environment-based paths
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "osaa-mvp")
LANDING_AREA_FOLDER = f"{S3_ENV}/landing"
TRANSFORMED_AREA_FOLDER = f"{S3_ENV}/transformed"
STAGING_AREA_PATH = f"{S3_ENV}/staging"

# Local copy of master data
LOCAL = True


# Log configuration details
def log_configuration() -> None:
    """Log the current configuration details for debugging and transparency."""
    logger.info("🔧 OSAA MVP Configuration Details")
    logger.info("   Project Configuration:")
    logger.info(f"   🗂️  Root Directory: {ROOT_DIR}")

    logger.info("   📂 Data Directories:")
    logger.info(f"      • Datalake: {DATALAKE_DIR}")
    logger.info(f"      • Raw Data: {RAW_DATA_DIR}")
    logger.info(f"      • Staging Data: {STAGING_DATA_DIR}")
    logger.info(f"      • Master Data: {MASTER_DATA_DIR}")

    logger.info("   🗃️  Database Configuration:")
    logger.info(f"      • Database Path: {DB_PATH}")

    logger.info("   🌐 Environment Settings:")
    logger.info(f"      • Target Environment: {TARGET}")
    logger.info(f"      • Username: {USERNAME}")
    logger.info(f"      • S3 Environment: {S3_ENV}")

    logger.info("   ☁️ S3 Configuration:")
    logger.info(f"      • Bucket Name: {S3_BUCKET_NAME}")
    logger.info(f"      • S3 Upload Enabled: {ENABLE_S3_UPLOAD}")
    logger.info(f"      • Landing Area: {LANDING_AREA_FOLDER}")
    logger.info(f"      • Transformed Area: {TRANSFORMED_AREA_FOLDER}")
    logger.info(f"      • Staging Area: {STAGING_AREA_PATH}")


# Call log configuration when the module is imported
log_configuration()
