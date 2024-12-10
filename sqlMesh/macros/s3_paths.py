"""S3 Path Generation Macros for SQLMesh Data Processing.

This module provides utility macros for generating S3 paths for data landing
and transformed datasets. It supports dynamic path generation based on
environment variables and fully qualified table names.

Key features:
- Generate S3 landing paths for raw data
- Generate S3 transformed paths for processed data
- Support for different environments (prod, int, dev)
- Flexible path construction with username and target support
"""

import os
import typing as t

from sqlglot import exp
from sqlmesh import macro


def parse_fully_qualified_name(fqtn: t.Union[str, exp.Expression]) -> t.Tuple[str, str, str]:
    """Parse a fully qualified table name into its components.

    Args:
        fqtn: Fully qualified table name in the format 'database.schema.table'

    Returns:
        A tuple of (database, schema, table)

    Raises:
        ValueError: If the table name is not in the correct format
    """
    # Convert SQLGlot expression to string
    if isinstance(fqtn, exp.Expression):
        if isinstance(fqtn, exp.Identifier):
            fqtn = fqtn.name
        else:
            fqtn = str(fqtn)

    parts = fqtn.split(".")
    if len(parts) != 3:
        raise ValueError("Fully qualified table name must be in the format 'database.schema.table'")
    database, schema, table = parts
    return database, schema, table


@macro()
def s3_landing_path(
    evaluator: t.Any, subfolder_filename: t.Union[str, exp.Expression]
) -> exp.Literal:
    """Construct S3 landing path.

    Args:
        evaluator: SQLMesh macro evaluator
        subfolder_filename: Name of the subfolder and filename

    Returns:
        S3 path as a SQLGlot literal expression
    """
    bucket = os.environ.get("S3_BUCKET_NAME", "osaa-mvp")
    target = os.environ.get("TARGET", "prod").lower()
    username = os.environ.get("USERNAME", "default").lower()

    # Construct the environment path segment
    env_path = target if target in ["prod", "int"] else f"{target}_{username}"

    # Convert input to string if it's a SQLGlot expression
    if isinstance(subfolder_filename, exp.Expression):
        subfolder_filename = str(subfolder_filename).strip("'")

    path = f"s3://{bucket}/{env_path}/landing/{subfolder_filename}.parquet"
    return exp.Literal.string(path)


@macro()
def s3_transformed_path(evaluator: t.Any, fqtn: t.Union[str, exp.Expression]) -> exp.Literal:
    """Construct S3 transformed path.

    Args:
        evaluator: SQLMesh macro evaluator
        fqtn: Fully qualified table name

    Returns:
        S3 path as a SQLGlot literal expression
    """
    bucket = os.environ.get("S3_BUCKET_NAME", "osaa-mvp")
    target = os.environ.get("TARGET", "dev").lower()
    username = os.environ.get("USERNAME", "default").lower()

    # Construct the environment path segment
    env_path = target if target in ["prod", "int"] else f"{target}_{username}"

    _, schema, table = parse_fully_qualified_name(fqtn)
    path = f"s3://{bucket}/{env_path}/transformed/{schema}/{table}.parquet"
    return exp.Literal.string(path)
