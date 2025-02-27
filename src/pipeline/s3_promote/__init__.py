"""S3 promote package for environment promotion.

This module handles the promotion of data between environments
in the United Nations OSAA MVP S3 storage structure.
"""

import logging
from pipeline.logging_config import create_logger

logger = create_logger(__name__)


def init_s3_promote_package() -> None:
    """Initialize the s3_promote package and log package details."""
    logger.info("🔄 Initializing OSAA MVP S3 Promote Package")
    logger.info("   📦 Package responsible for environment promotion")
    logger.info("   🔍 Ready to promote between environments")


# Call initialization when the package is imported
init_s3_promote_package()