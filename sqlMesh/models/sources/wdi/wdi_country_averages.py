from sqlmesh.core.macros import MacroEvaluator
from sqlmesh import model
import ibis
from constants import DB_PATH
from macros.utils import create_empty_result

@model(
    "sources.wdi_country_averages",
    is_sql=True,
    kind="FULL",
    columns={
        "country_id": "String",
        "indicator_id": "String",
        "year": "Int",
        "value": "Decimal",
        "magnitude": "String",
        "qualifier": "String",
        "indicator_description": "String",
        "avg_value_by_country": "Float",
    }
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Calculate country averages for WDI indicators."""
    try:
        # Connect to DuckDB
        con = ibis.connect(f"duckdb://{DB_PATH}")
        
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
        return create_empty_result({
            "country_id": "String",
            "indicator_id": "String",
            "year": "Int",
            "value": "Decimal",
            "magnitude": "String",
            "qualifier": "String",
            "indicator_description": "String",
            "avg_value_by_country": "Float",
        })
