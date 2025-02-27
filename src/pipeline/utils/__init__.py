"""Utility functions for the pipeline package.

This module contains utility functions and scripts used across the pipeline.
"""

from pipeline.logging_config import create_logger

logger = create_logger(__name__)

def init_utils_package():
    """Initialize the utils package."""
    logger.info("🔧 Initializing pipeline utils package")

# Call initialization when the package is imported
init_utils_package() 