import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.utils import find_indicator_models, create_empty_result
from constants import DB_PATH

@model(
    "master.indicators",
    is_sql=True,
    kind="FULL",
    columns={
        "indicator_id": "String",
        "country_id": "String",
        "year": "Int64",
        "value": "Decimal",
        "magnitude": "String",
        "qualifier": "String",
        "indicator_description": "String",
        "source": "String",
    },
    post_statements=["@s3_write()"]
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """
    Create a unified indicators table from all indicator sources.
    """
    try:
        # Connect to DuckDB directly
        con = ibis.connect(f"duckdb://{DB_PATH}")
        
        # Discover all source tables dynamically
        source_tables = []
        
        # First, find all indicator model source directories
        db_tables = con.list_tables(like='sources.%')
        
        print(f"Found the following source tables: {db_tables}")
        
        # Try to get each source table
        for table_name in db_tables:
            if table_name.startswith('sources.'):
                source_name = table_name.split('.')[1]
                try:
                    source_table = con.table(table_name)
                    # Add a source column with the source name
                    source_tables.append(source_table.mutate(source=ibis.literal(source_name)))
                    print(f"Successfully loaded source table: {table_name}")
                except Exception as e:
                    print(f"Warning: Could not access {table_name} table: {e}")
        
        # Union all tables
        if not source_tables:
            # Return an empty result set with the correct schema if no tables were found
            print("No source tables found, returning empty result")
            return create_empty_result({
                "indicator_id": "String",
                "country_id": "String",
                "year": "Int64",
                "value": "Decimal",
                "magnitude": "String",
                "qualifier": "String",
                "indicator_description": "String",
                "source": "String",
            })
        
        unioned_t = ibis.union(*source_tables).order_by(["year", "country_id", "indicator_id"])
        return ibis.to_sql(unioned_t)
    
    except Exception as e:
        print(f"Error creating master indicators table: {e}")
        return create_empty_result({
            "indicator_id": "String",
            "country_id": "String",
            "year": "Int64",
            "value": "Decimal",
            "magnitude": "String",
            "qualifier": "String",
            "indicator_description": "String",
            "source": "String",
        })
