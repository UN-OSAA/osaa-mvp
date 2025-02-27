"""Pipeline package for the United Nations OSAA MVP project.

This package contains modules for data ingestion, transformation, and
management for the United Nations OSAA MVP data pipeline.
"""

import logging
import os
import sys

from pipeline.logging_config import create_logger, configure_logging

# Initialize logging
logger = create_logger(__name__)

# Call configuration function
configure_logging()

def init_pipeline_package() -> None:
    """Initialize the pipeline package and log package details."""
    logger.info("🚀 Initializing OSAA MVP Pipeline")
    logger.info("📦 Modules: Ingestion, Upload, Utilities")

    package_path = os.path.dirname(os.path.abspath(__file__))
    logger.info(f"📂 Package Path: {package_path}")


# Call initialization when the package is imported
init_pipeline_package()
