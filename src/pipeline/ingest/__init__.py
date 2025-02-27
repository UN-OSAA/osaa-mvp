"""Ingest package for data import and processing.

This module handles the ingestion of data from various sources
into the United Nations OSAA MVP project data pipeline.
"""

import logging
from pipeline.logging_config import create_logger

logger = create_logger(__name__)


def init_ingest_package() -> None:
    """Initialize the ingest package and log package details."""
    logger.info("🚢 Initializing OSAA MVP Ingest Package")
    logger.info("   📦 Package responsible for data import and processing")
    logger.info("   🔍 Ready to ingest data from various sources")


# Call initialization when the package is imported
init_ingest_package()
