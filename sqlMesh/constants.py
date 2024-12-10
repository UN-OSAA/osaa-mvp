"""Constants and configuration for the SQLMesh data processing pipeline.

This module defines key directory paths and database configuration for the
OSAA MVP project. It provides a centralized location for managing file paths
and environment-specific settings.

Key features:
- Determine the absolute path to the SQLMesh directory
- Provide a configurable database path with environment variable support
"""

import os

# Get the absolute path to the SQLMesh directory
SQLMESH_DIR = os.path.dirname(os.path.abspath(__file__))

# Use environment variable with fallback to local path
DB_PATH = os.getenv("DB_PATH", os.path.join(SQLMESH_DIR, "osaa_mvp.db"))
