import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh import model
from constants import DB_PATH
from macros.utils import create_empty_result

@model(
    "sources.opri",
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
    }
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Process OPRI data and return the transformed Ibis table."""
    
    source_folder_path = "opri"

    try:
        # Connect to DuckDB
        con = ibis.connect(f"duckdb://{DB_PATH}")
        
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
