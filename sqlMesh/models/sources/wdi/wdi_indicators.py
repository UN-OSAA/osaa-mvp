import ibis
import re
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh import model
from macros.ibis_expressions import generate_ibis_table
from macros.utils import get_sql_model_schema
from sqlglot import exp
import typing as t
from typing import Optional, Dict, Union, Any
import ibis.selectors as s

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

@model(
    "sources.wdi",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Process WDI data and return the transformed Ibis table."""
    
    source_folder_path = "wdi"
    
    try:
        # Connect to DuckDB
        con = ibis.connect("duckdb:///app/sqlMesh/unosaa_data_pipeline.db")
        
        # Try to get the tables directly
        csv_table = con.table("wdi.csv")
        series_table = con.table("wdi.series")
        
        # Process the CSV data
        wdi_data = (
            csv_table.rename("snake_case")
            .rename(country_id="country_code", indicator_id="indicator_code")
            .select("country_id", "indicator_id", s.numeric())
            .pivot_longer(s.index["1960":], names_to="year", values_to="value")
            .cast({"year": "int64", "value": "decimal"})
        )
        
        # Process the series data
        wdi_series_renamed = (
            series_table
            .rename("snake_case")
            .rename(indicator_id="series_code")
        )
        
        # Join the tables
        wdi = (
            wdi_data.left_join(
                wdi_series_renamed,
                "indicator_id"
            )
            .mutate(
                magnitude=ibis.literal(""),
                qualifier=ibis.literal(""),
                indicator_description=wdi_series_renamed["long_definition"]
            )
        )
        
        return ibis.to_sql(wdi)
        
    except Exception as e:
        # If any error occurs, return an empty table with the right schema
        print(f"Error processing WDI data: {e}")
        empty_df_sql = """
        SELECT 
            '' AS country_id,
            '' AS indicator_id,
            0 AS year,
            0.0 AS value,
            '' AS magnitude,
            '' AS qualifier,
            '' AS indicator_description
        WHERE 1=0
        """
        return empty_df_sql
