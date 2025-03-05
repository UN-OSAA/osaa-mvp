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
from constants import DB_PATH

SQLMESH_DIR = '/app/sqlMesh'

@model(
    "sources.sdg",
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
    },
    description="""This model contains Sustainable Development Goals (SDG) data for all countries and indicators.""",
    column_descriptions={
        "indicator_id": "The unique identifier for the indicator",
        "country_id": "The unique identifier for the country",
        "year": "The year of the data",
        "value": "The value of the indicator for the country and year",
        "magnitude": "The magnitude of the indicator for the country and year",
        "qualifier": "The qualifier of the indicator for the country and year",
        "indicator_description": "The description of the indicator",
    },
    grain=("indicator_id", "country_id", "year"),
    physical_properties={
        "publishing_org": "UN",
        "link_to_raw_data": "https://unstats.un.org/sdgs/dataportal",
        "dataset_owner": "UN",
        "dataset_owner_contact_info": "https://unstats.un.org/sdgs/contact-us/",
        "funding_source": "UN",
        "maintenance_status": "Actively Maintained",
        "how_data_was_collected": "https://unstats.un.org/sdgs/dataContacts/",
        "update_cadence": "Annually",
        "transformations_of_raw_data": "indicator labels and descriptions are joined together",
    },
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Process SDG data and return the transformed Ibis table."""
    
    source_folder_path = "sdg"

    try:
        # Connect to DuckDB
        con = ibis.connect(f"duckdb://{DB_PATH}")
        
        # Try to get the tables directly
        data_national_table = con.table("sdg.data_national")
        label_table = con.table("sdg.label")
        
        # Process the data
        sdg_table = (
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
        
        return ibis.to_sql(sdg_table)
        
    except Exception as e:
        # If any error occurs, return an empty table with the right schema
        print(f"Error processing SDG data: {e}")
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
