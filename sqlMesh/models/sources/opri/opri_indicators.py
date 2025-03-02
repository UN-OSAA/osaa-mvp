import ibis
import re
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh import model
from macros.ibis_expressions import generate_ibis_table
from macros.utils import get_sql_model_schema
from sqlglot import exp
import typing as t
from typing import Optional, Dict, Union, Any

COLUMN_SCHEMA = {
    "country_id": "String",
    "indicator_id": "String",
    "year": "Int",
    "value": "Decimal",
    "magnitude": "String",
    "qualifier": "String",
    "indicator_description": "String",
}

SQLMESH_DIR = '/app/sqlMesh'

def _convert_duckdb_type_to_ibis(duckdb_type):
    type_str = str(duckdb_type).upper()
    base_type = type_str.split("(")[0].strip()
    type_mapping = {'TEXT': 'String', 'VARCHAR': 'String', 'CHAR': 'String',
        'INT': 'Int', 'INTEGER': 'Int', 'BIGINT': 'Int', 'DECIMAL':
        'Decimal', 'NUMERIC': 'Decimal'}
    return type_mapping.get(base_type, 'String')

@model(
    "sources.opri",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Process OPRI data and return the transformed Ibis table."""
    
    source_folder_path = "opri"

    try:
        # Connect to DuckDB
        con = ibis.connect("duckdb:///app/sqlMesh/unosaa_data_pipeline.db")
        
        # Try to get the tables directly
        data_national_table = con.table("opri.data_national")
        label_table = con.table("opri.label")
        
        # Process the data
        opri_table = (
            data_national_table.left_join(label_table, "indicator_id")
            .select(
                "indicator_id",
                "country_id",
                "year",
                "value",
                "magnitude",
                "qualifier",
                "indicator_label_en",
            )
            .rename(indicator_description="indicator_label_en")
        )
        
        return ibis.to_sql(opri_table)
        
    except Exception as e:
        # If any error occurs, return an empty table with the right schema
        print(f"Error processing OPRI data: {e}")
        empty_df_sql = """
        SELECT 
            '' AS indicator_id,
            '' AS country_id,
            0 AS year,
            0.0 AS value,
            '' AS magnitude,
            '' AS qualifier,
            '' AS indicator_description
        WHERE 1=0
        """
        return empty_df_sql
