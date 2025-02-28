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
    try:
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
                
                # If we got a SQL string back instead of an ibis table,
                # this means the table doesn't exist yet - skip it
                if isinstance(table, str):
                    print(f"Warning: Table for {source} doesn't exist yet, skipping")
                    continue
                    
                # Add source column
                tables.append(table.mutate(source=ibis.literal(source)))
            except ImportError:
                print(f"Warning: Could not import module: {module_name}")
            except Exception as e:
                print(f"Warning: Error processing {module_name}: {str(e)}")

        # Union all tables
        if not tables:
            # Return an empty result set with the correct schema if no tables were found
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
            
            # Create a SQL query that returns an empty result set with the correct schema
            return f"SELECT {', '.join(columns)} WHERE 1=0"

        unioned_t = ibis.union(*tables).order_by(["year", "country_id", "indicator_id"])
        return ibis.to_sql(unioned_t)
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
        print(f"Error in master indicators: {str(e)}")
        
        # Create a SQL query that returns an empty result set with the correct schema
        query = f"SELECT {', '.join(columns)} WHERE 1=0"
        
        return query
