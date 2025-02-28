import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from constants import DB_PATH
import time
import random
import os


COLUMN_SCHEMA = {
    "model_name": "String",
    "model_description": "String",
    "model_kind": "String",
    "grain": "String",
    "columns": "String",
    "column_descriptions": "String",
    "physical_properties": "String",
}


@model(
    "_metadata.all_models",
    is_sql=True,
    kind="FULL",
    depends_on=["unosaa_data_pipeline.master.indicators", "unosaa_data_pipeline.sqlmesh._snapshots"],
    columns=COLUMN_SCHEMA,
    post_statements=["@s3_write()"],
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """
    This model is used to get the model properties of the latest snapshot.
    The user may see the following warning the first time they run the project, but they can safely ignore it:
    ```
    2025-01-21 20:39:23,269 - MainThread - sqlmesh.core.renderer - WARNING - SELECT * cannot be expanded due to missing schema(s) for model(s): '"unbound_table_1"'.
    Run `sqlmesh create_external_models` and / or make sure that the model '"osaa_mvp"."_metadata"."all_models"' can be rendered at parse time. (renderer.py:540)
    ```
    """
    # Multiple database paths to try with retries
    db_paths = [
        DB_PATH,
        "/app/sqlMesh/unosaa_data_pipeline.db",
        "/app/sqlMesh/data/db/sqlmesh.db"
    ]
    
    max_retries = 5
    for retry in range(max_retries):
        for db_path in db_paths:
            if not os.path.exists(db_path):
                continue
                
            try:
                con = ibis.connect(f"duckdb://{db_path}")
                
                query = """
                    SELECT
                        json_extract(s.snapshot, '$.node.name') AS model_name,
                        json_extract(s.snapshot, '$.node.description') AS model_description,
                        json_extract(s.snapshot, '$.node.kind') AS model_kind,
                        json_extract(s.snapshot, '$.node.grains') AS grain,
                        json_extract(s.snapshot, '$.node.columns') AS columns,
                        json_extract(s.snapshot, '$.node.column_descriptions') AS column_descriptions,
                        json_extract(s.snapshot, '$.node.physical_properties') AS physical_properties
                    FROM 
                        sqlmesh._snapshots s
                    INNER JOIN (
                        SELECT name, MAX(updated_ts) AS max_updated_ts
                        FROM sqlmesh._snapshots
                        GROUP BY name
                    ) latest ON s.name = latest.name AND s.updated_ts = latest.max_updated_ts
                    ;
                """

                t = con.sql(query)
                return ibis.to_sql(t)
            except Exception as e:
                print(f"Error accessing database {db_path}: {str(e)}")
                
        # If we reach here, all database paths failed on this retry
        # Add exponential backoff with jitter before retrying
        if retry < max_retries - 1:
            sleep_time = (2 ** retry) + random.random()
            print(f"Retrying metadata database connection in {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
    
    # If all retries fail, return an empty table with the correct schema
    print("All database connection attempts failed for metadata, returning empty table")
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
    
    return f"SELECT {', '.join(columns)} WHERE 1=0"
