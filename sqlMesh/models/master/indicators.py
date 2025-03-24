import ibis
import os
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.ibis_expressions import generate_ibis_table
from macros.utils import find_indicator_models

COLUMN_SCHEMA = {
    "indicator_id": "String",
    "country_id": "String",
    "year": "Int64",
    "value": "Decimal",
    "magnitude": "String",
    "qualifier": "String",
    "indicator_description": "String",
}


@model(
    "master.indicators",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    post_statements=["@s3_write()"]
)



def entrypoint(evaluator: MacroEvaluator) -> str:
    indicator_models = find_indicator_models()

    # Import each model and get its table
    tables = []
    for source, module_name in indicator_models:
        try:
            # Dynamically import the module
            module = __import__(module_name, fromlist=["COLUMN_SCHEMA"])

            # Generate table for this source
            table = generate_ibis_table(
                evaluator,
                table_name=source,
                schema_name="sources",
                column_schema=module.COLUMN_SCHEMA,
            )
            # Add source column
            tables.append(table.mutate(source=ibis.literal(source)))
        except ImportError:
            raise ImportError(f"Could not import module: {module_name}")
        except AttributeError:
            raise AttributeError(f"Module {module_name} does not have COLUMN_SCHEMA")

    # Union all tables
    if not tables:
        raise ValueError("No indicator models found")

    unioned_t = ibis.union(*tables).order_by(["year", "country_id", "indicator_id"])
    return ibis.to_sql(unioned_t)
