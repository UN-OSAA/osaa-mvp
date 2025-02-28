"""Pipeline package for the United Nations OSAA MVP project.

This package contains modules for data ingestion, transformation, and
management for the United Nations OSAA MVP data pipeline.
"""

import logging
import os
import sys

from pipeline.logging_config import create_logger

# Initialize logging
logger = create_logger(__name__)

# No need to call configure_logging as it doesn't exist

def init_pipeline_package() -> None:
    """Initialize the pipeline package and log package details."""
    logger.info("🚀 Initializing OSAA MVP Pipeline")
    logger.info("📦 Modules: Ingestion, Upload, Utilities")

    package_path = os.path.dirname(os.path.abspath(__file__))
    logger.info(f"📂 Package Path: {package_path}")


# Call initialization when the package is imported
init_pipeline_package()
