"""Utility functions for the pipeline package.

This module contains utility functions and scripts used across the pipeline.
"""

from typing import Optional, Tuple, Any
from pipeline.logging_config import create_logger
from pipeline.utils.s3_helpers import s3_init

# Re-export the types needed for s3_init's return value signature
__all__ = ['s3_init', 'Optional', 'Tuple', 'Any']

logger = create_logger(__name__)

def init_utils_package():
    """Initialize the utils package."""
    logger.info("🔧 Initializing pipeline utils package")

# Call initialization when the package is imported
init_utils_package() 