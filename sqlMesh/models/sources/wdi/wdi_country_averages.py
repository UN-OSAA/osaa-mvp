from sqlmesh.core.macros import MacroEvaluator
from sqlmesh import model
import ibis
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
    try:
        # Connect to DuckDB
        con = ibis.connect("duckdb:///app/sqlMesh/unosaa_data_pipeline.db")
        
        # Try to get the WDI table directly
        wdi = con.table("sources.wdi")
        
        # Calculate country averages
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
        
    except Exception as e:
        # If any error occurs, return an empty table with the right schema
        print(f"Error processing WDI country averages: {e}")
        empty_df_sql = """
        SELECT 
            '' AS country_id,
            '' AS indicator_id,
            0 AS year,
            0.0 AS value,
            '' AS magnitude,
            '' AS qualifier,
            '' AS indicator_description,
            0.0 AS avg_value_by_country
        WHERE 1=0
        """
        return empty_df_sql
