import ibis
import os
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.utils import generate_ibis_table, find_indicator_models
import typing as t
from typing import Optional, Dict, Union, Any

COLUMN_SCHEMA = {
    "indicator_id": "String",
    "country_id": "String",
    "year": "Int64",
    "value": "Decimal",
    "magnitude": "String",
    "qualifier": "String",
    "indicator_description": "String",
    "source": "String",
}

# Default schema to use if a module doesn't define one
DEFAULT_COLUMN_SCHEMA = {
    "country_id": "String",
    "indicator_id": "String",
    "year": "Int",
    "value": "Decimal",
    "magnitude": "String",
    "qualifier": "String",
}


@model(
    "master.indicators",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    post_statements=["@s3_write()"]
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """
    Create a unified indicators table from all indicator sources.
    """
    indicator_models = find_indicator_models(
        [
            "opri",
            "sdg",
            "wdi",
        ]
    )

    # Import each model and get its table
    tables = []
    for source, module_name in indicator_models:
        try:
            # Dynamically import the module
            module = __import__(module_name, fromlist=["COLUMN_SCHEMA"])

            # Get column schema, use default if not available
            try:
                column_schema = module.COLUMN_SCHEMA
            except AttributeError:
                print(f"Warning: Module {module_name} does not have COLUMN_SCHEMA, using default")
                column_schema = DEFAULT_COLUMN_SCHEMA

            # Generate table for this source
            table = generate_ibis_table(
                evaluator,
                table_name=source,
                schema_name="sources",
                column_schema=column_schema,
            )
            
            # Add the table to our list
            tables.append(table.mutate(source=ibis.literal(source)))
        except ImportError:
            print(f"Warning: Could not import module: {module_name}")
        except Exception as e:
            print(f"Warning: Error processing {module_name}: {str(e)}")

    # Union all tables
    if not tables:
        # Return an empty result set with the correct schema if no tables were found
        return "SELECT NULL AS indicator_id, NULL AS country_id, NULL AS year, NULL AS value, NULL AS magnitude, NULL AS qualifier, NULL AS indicator_description, NULL AS source WHERE 1=0"

    unioned_t = ibis.union(*tables).order_by(["year", "country_id", "indicator_id"])
    return ibis.to_sql(unioned_t)
