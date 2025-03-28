from sqlmesh import macro
import re
from constants import SQLMESH_DIR
import os
from sqlmesh.core.macros import macro, MacroEvaluator
from sqlglot import exp
import typing as t


def _convert_duckdb_type_to_ibis(duckdb_type):
    # Convert to string and uppercase for consistency
    type_str = str(duckdb_type).upper()

    # Extract base type by removing anything after '(' if it exists
    # Handling dtype such as DECIMAL(18,3)
    base_type = type_str.split("(")[0].strip()

    type_mapping = {
        "TEXT": "String",
        "VARCHAR": "String",
        "CHAR": "String",
        "INT": "Int",
        "INTEGER": "Int",
        "BIGINT": "Int",
        "DECIMAL": "Decimal",
        "NUMERIC": "Decimal",
    }
    return type_mapping.get(base_type, "String")  # Default to String if unknown


@macro()
def get_sql_model_schema(evaluator, sql_file_name, folder_path_from_models_folder):
    """Get schema from a SQL model file.

    Args:
        evaluator: SQLMesh evaluator instance
        sql_file_name: Name of the SQL file without extension
        folder_path_from_models_folder: Path from models folder (e.g. 'edu' or 'wdi')
                                      The path should match the source data folder
    """
    file_path = f"{SQLMESH_DIR}/models/sources/{folder_path_from_models_folder}/{sql_file_name.lower()}.sql"
    with open(file_path, "r") as file:
        sql_content = file.read()

    # Regular expression to match the MODEL section
    model_pattern = re.compile(
        r"MODEL\s*\([\s\S]*?columns\s*\(\s*([\s\S]*?)\s*\)[\s\S]*?\)", re.IGNORECASE
    )
    match = model_pattern.search(sql_content)

    if not match:
        return {}

    columns_section = match.group(1)

    # Regular expression to extract column name and type
    # This pattern now accounts for column names with spaces, assuming they are quoted
    column_pattern = re.compile(r'"?([\w\s]+)"?\s+(\w+)', re.IGNORECASE)
    columns = column_pattern.findall(columns_section)

    # Convert list of tuples to dictionary
    columns_dict = {
        name.strip().lower(): _convert_duckdb_type_to_ibis(str(col_type))
        for name, col_type in columns
    }

    return columns_dict


@macro()
def s3_read(
    evaluator: t.Any, subfolder_filename: t.Union[str, exp.Expression]
) -> exp.Literal:
    """Generate S3 path for reading data from the landing zone.

    Args:
        evaluator: SQLMesh macro evaluator
        subfolder_filename: Subfolder and filename without extension (e.g. 'edu/OPRI_LABEL')

    Returns:
        S3 path as SQLGlot string literal
        Example: 's3://bucket/dev/dev_user/landing/edu/OPRI_LABEL.parquet'

    Environment:
        S3_BUCKET_NAME (str): Bucket name (default: "unosaa-data-pipeline")
        TARGET (str): prod or dev (default: "dev")
    """
    bucket = os.environ.get("S3_BUCKET_NAME", "unosaa-data-pipeline")
    target = os.environ.get("TARGET", "dev").lower()

    # Convert input to string if it's a SQLGlot expression
    if isinstance(subfolder_filename, exp.Expression):
        subfolder_filename = str(subfolder_filename).strip("'")

    path = f"s3://{bucket}/{target}/landing/{subfolder_filename}.parquet"
    return exp.Literal.string(path)


@macro()
def s3_write(evaluator: MacroEvaluator) -> str:
    """Generate COPY statement for writing model data to the staging zone.

    Args:
        evaluator: SQLMesh macro evaluator containing model context

    Returns:
        DuckDB COPY statement
        Example: COPY (SELECT * FROM table) TO 's3://bucket/dev/staging/source/table.parquet'

    Environment:
        S3_BUCKET_NAME (str): Bucket name (default: "unosaa-data-pipeline")
        TARGET (str): prod or dev (default: "dev")
        USERNAME (str): Used in dev paths (default: "default")
        DRY_RUN_FLG (str): Enable/disable dry run (default: "false")

    Note: Handles SQLMesh physical table names by removing hash suffixes and comments.
    """

    # Check if dry run is enabled
    dry_run_flg = os.environ.get("DRY_RUN_FLG", "false").lower() == "true"
    if dry_run_flg:
        return None  # Return empty string to skip S3 upload

    # Handling the dynamic nature of the schema/table name in sqlmesh
    # It changes depending on the runtime stage. 
    if evaluator.locals.get("runtime_stage") != "loading":
       
        # Get environment variables
        bucket = os.environ.get("S3_BUCKET_NAME", "unosaa-data-pipeline")
        target = os.environ.get("TARGET", "dev").lower()
        username = os.environ.get("USERNAME", "default").lower()

        # Construct environment path
        env_path = "prod" if target == "prod" else f"{target}_{username}"

        # Get and parse model name
        this_model = str(evaluator.locals.get("this_model", ""))
        full_table_name = this_model.split(".")[2].strip('"').split()[0].rsplit("__", 1)[0] 
        schema, table_name = full_table_name.split("__", 1)  

        # Determine directory path based on schema
        schema_path = "master" if schema == "master" else "_metadata" if schema == "_metadata" else "source"
        dir = schema + "/" if schema != schema_path else ""

        # Construct S3 path
        if target == "dev":
            s3_path = f"s3://{bucket}/dev/staging/{env_path}/{schema_path}/{dir}{table_name}.parquet"    
        else:
            s3_path = f"s3://{bucket}/{env_path}/staging/{schema_path}/{dir}{table_name}.parquet"

        # Build the SQL statement
        sql = f"""COPY (SELECT * FROM {this_model}) TO '{s3_path}' (FORMAT PARQUET)"""

        return sql


def find_indicator_models(
    selected_models: t.Optional[t.List[str]] = None,
) -> t.List[t.Tuple[str, str]]:
    """Find all models ending with _indicators in the sources directory.

    Args:
        selected_models: Optional list of model names to include (e.g., ['opri']).
                         If None, all models ending with _indicators.py are included.
                         If source names are provided, only models ending with _indicators.py in the selected sources will be included.
                         If a model name is not found, it will be skipped.
    """
    indicator_models = []
    sources_dir = os.path.join(SQLMESH_DIR, "models", "sources")

    try:
        for source in os.listdir(sources_dir):
            source_dir = os.path.join(sources_dir, source)
            if os.path.isdir(source_dir):
                indicator_files = [
                    f for f in os.listdir(source_dir) if f.endswith("_indicators.py")
                ]
                for file in indicator_files:
                    module_name = f"models.sources.{source}.{file[:-3]}"
                    # Check if the module_name is in the selected_models list
                    if selected_models is None or source in selected_models:
                        indicator_models.append((source, module_name))
    except FileNotFoundError:
        raise FileNotFoundError(f"Sources directory not found: {sources_dir}")
    except Exception as e:
        raise RuntimeError(f"An error occurred while finding indicator models: {e}")

    return indicator_models
