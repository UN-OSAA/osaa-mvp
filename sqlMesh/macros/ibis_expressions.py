"""
Ibis expression utilities for SQLMesh models.

This module provides functions to generate and work with Ibis expressions
for data processing in SQLMesh models.
"""

import ibis
from sqlmesh.core.macros import MacroEvaluator


def generate_ibis_table(evaluator: MacroEvaluator, table_name: str, column_schema: dict, schema_name: str = None):
    """
    Generate an Ibis table expression from a SQL model using DuckDB.
    
    Args:
        evaluator: MacroEvaluator instance
        table_name: Name of the table
        column_schema: Dictionary defining the column schema
        schema_name: Optional schema name
    
    Returns:
        Ibis table expression
    """
    # If there's a schema, prepend it to the table name
    full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
    
    # Create a query to select from the table
    query = f"SELECT * FROM {full_table_name}"
    
    # Evaluate the query and return the result
    return evaluator.evaluate_with_ibis(query) 