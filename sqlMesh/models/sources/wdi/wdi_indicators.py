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

def _convert_duckdb_type_to_ibis(duckdb_type):
    type_str = str(duckdb_type).upper()
    base_type = type_str.split("(")[0].strip()
    type_mapping = {'TEXT': 'String', 'VARCHAR': 'String', 'CHAR': 'String',
        'INT': 'Int', 'INTEGER': 'Int', 'BIGINT': 'Int', 'DECIMAL':
        'Decimal', 'NUMERIC': 'Decimal'}
    return type_mapping.get(base_type, 'String')

@model(
    "sources.wdi",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Process WDI data and return the transformed Ibis table."""
    
    source_folder_path = "wdi"
    wdi_csv = generate_ibis_table(
        evaluator,
        table_name="csv",
        column_schema=get_sql_model_schema(evaluator, "csv", source_folder_path),
        schema_name="wdi",
    )

    wdi_data = (
        wdi_csv.rename("snake_case")
        .rename(country_id="country_code", indicator_id="indicator_code")
        .select("country_id", "indicator_id", s.numeric())
        .pivot_longer(s.index["1960":], names_to="year", values_to="value")
        .cast({"year": "int64", "value": "decimal"})
    )

    wdi_series = generate_ibis_table(
        evaluator,
        table_name="series",
        column_schema=get_sql_model_schema(evaluator, "series", source_folder_path),
        schema_name="wdi",
    )

    wdi_series_renamed = (
        wdi_series
        .rename("snake_case")
        .rename(indicator_id="series_code")
    )

    wdi = (
        wdi_data.left_join(
            wdi_series_renamed,
            "indicator_id"
        )
        .mutate(
            magnitude=ibis.literal(""),  # Empty string for now
            qualifier=ibis.literal(""),  # Empty string for now
            indicator_description=wdi_series_renamed["long_definition"]
        )
    )

    return ibis.to_sql(wdi)
