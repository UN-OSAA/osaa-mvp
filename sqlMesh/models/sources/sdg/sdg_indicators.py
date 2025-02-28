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
    "sources.sdg",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
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
    
    try:
        source_folder_path = "sdg"

        sdg_data_national = generate_ibis_table(
            evaluator,
            table_name="data_national",
            column_schema=get_sql_model_schema(evaluator, "data_national", source_folder_path),
            schema_name="sdg",
        )

        sdg_label = generate_ibis_table(
            evaluator,
            table_name="label",
            column_schema=get_sql_model_schema(evaluator, "label", source_folder_path),
            schema_name="sdg",
        )

        sdg_table = (
            sdg_data_national.left_join(sdg_label, "indicator_id")
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
        # If there's an error, return an empty table with the correct schema
        columns = []
        for col_name, col_type in COLUMN_SCHEMA.items():
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
        
        # Log the error
        print(f"Error processing SDG indicators: {str(e)}")
        
        # Create a SQL query that returns an empty result set with the correct schema
        query = f"SELECT {', '.join(columns)} WHERE 1=0"
        
        return query
