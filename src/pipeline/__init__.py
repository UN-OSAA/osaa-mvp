"""Pipeline package for data processing and management.

This package contains modules for data ingestion and utility functions
used in the United Nations OSAA MVP project.
"""

import logging
import os
import sys


# Configure logging for the entire package
def setup_package_logging() -> logging.Logger:
    """Set up comprehensive logging for the pipeline package."""
    # Create a package-level logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(console_handler)

    return logger


# Initialize package logging
logger = setup_package_logging()


def init_pipeline_package() -> None:
    """Initialize the pipeline package and log package details."""
    logger.info("🚀 Initializing OSAA MVP Pipeline Package")
    logger.info("   🌐 United Nations OSAA MVP Data Processing Pipeline")
    logger.info("   📦 Modules:")
    logger.info("      • Data Ingestion")
    logger.info("      • Utility Functions")

    # Log package path for debugging
    package_path = os.path.dirname(os.path.abspath(__file__))
    logger.info(f"   📂 Package Path: {package_path}")


# Call initialization when the package is imported
init_pipeline_package()
