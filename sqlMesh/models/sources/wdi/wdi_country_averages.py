from sqlmesh.core.macros import MacroEvaluator
from sqlmesh import model
import ibis
from macros.ibis_expressions import generate_ibis_table
from sqlglot import exp
from models.sources.wdi.wdi_indicators import COLUMN_SCHEMA as WDI_COLUMN_SCHEMA

COLUMN_SCHEMA = {
    "country_id": "String",
    "indicator_id": "String",
    "year": "Int",
    "value": "Decimal",
    "magnitude": "String",
    "qualifier": "String",
    "indicator_description": "String",
    "avg_value_by_country": "Float",
}


@model(
    "sources.wdi_country_averages",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Calculate country averages for WDI indicators."""
    # Get the WDI table
    wdi = generate_ibis_table(
        evaluator,
        table_name="wdi",
        schema_name="sources",
        column_schema=WDI_COLUMN_SCHEMA,
    )
    
    # If we got a string back (empty table SQL), just return it
    if isinstance(wdi, str):
        # Modify the SQL to include all our columns including avg_value_by_country
        columns = []
        for col_name, col_type in COLUMN_SCHEMA.items():
            if col_type in ["String"]:
                sql_type = "VARCHAR"
            elif col_type in ["Int", "Int64"]:
                sql_type = "INTEGER"
            elif col_type in ["Decimal"]:
                sql_type = "DECIMAL(18,3)"
            elif col_type in ["Float"]:
                sql_type = "FLOAT"
            else:
                sql_type = "VARCHAR"
            columns.append(f"CAST(NULL AS {sql_type}) AS {col_name}")
        return f"SELECT {', '.join(columns)} WHERE 1=0"

    # Otherwise, calculate country averages
    country_averages = (
        wdi.filter(wdi.value.notnull())
        .group_by(["country_id", "indicator_id"])
        .agg(avg_value_by_country=wdi.value.mean())
        .join(wdi, ["country_id", "indicator_id"])
        .select(
            "country_id",
            "indicator_id",
            "year",
            "value",
            "magnitude",
            "qualifier",
            "indicator_description",
            "avg_value_by_country"
        )
    )

    return ibis.to_sql(country_averages)
