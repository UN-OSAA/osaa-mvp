"""
Ibis expression utilities for SQLMesh models.

This module provides functions to generate and work with Ibis expressions
for data processing in SQLMesh models.
"""

from typing import Dict, Optional, Union
import ibis
from sqlmesh.core.macros import MacroEvaluator


def generate_ibis_table(
    evaluator: MacroEvaluator,
    table_name: str,
    schema_name: Optional[str] = None,
    column_schema: Optional[Dict[str, str]] = None,
) -> Union[ibis.Table, str]:
    """
    Generate an ibis table from a table name and schema name.
    If the table doesn't exist yet, this will return a SQL string that creates an empty table.

    Args:
        evaluator: The macro evaluator.
        table_name: The name of the table.
        schema_name: The name of the schema.
        column_schema: The schema of the table.

    Returns:
        An ibis table or a SQL string.
    """
    # If column_schema is not provided, we can't create an empty table
    if not column_schema:
        raise ValueError(f"Column schema is required for table {table_name}")
    
    # If there's a schema, prepend it to the table name
    full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
    
    # Try to connect to the database and get the table
    try:
        con = ibis.connect("duckdb:///app/sqlMesh/unosaa_data_pipeline.db")
        sql = f"SELECT * FROM {full_table_name} LIMIT 1"
        table = con.sql(sql)
        
        # If we got here, the table exists
        # Create a full query to get all data
        sql = f"SELECT * FROM {full_table_name}"
        table = con.sql(sql)
        
        # If column_schema is provided, cast the columns
        casted_columns = {}
        for col_name, col_type in column_schema.items():
            if col_name in table.columns:
                # Cast the column to the specified type
                casted_columns[col_name] = table[col_name].cast(col_type).name(col_name)
            else:
                # If the column doesn't exist, create a null column
                casted_columns[col_name] = ibis.literal(None).cast(col_type).name(col_name)
        
        # Select the casted columns
        return table.select(list(casted_columns.values()))
    except Exception as e:
        # Table doesn't exist, return a SQL string to create an empty table
        print(f"Warning: Could not access table {full_table_name}: {str(e)}")
        
        # Create a SQL query that returns an empty result set with the correct schema
        columns = []
        for col_name, col_type in column_schema.items():
            # Map ibis types to SQL types
            if col_type in ["String"]:
                sql_type = "VARCHAR"
            elif col_type in ["Int", "Int64"]:
                sql_type = "INTEGER"
            elif col_type in ["Decimal"]:
                sql_type = "DECIMAL(18,3)"
            else:
                sql_type = "VARCHAR"
            
            # Add the column to the list
            columns.append(f"CAST(NULL AS {sql_type}) AS {col_name}")
        
        # Create a SQL query that returns an empty result set with the correct schema
        return f"SELECT {', '.join(columns)} WHERE 1=0" 