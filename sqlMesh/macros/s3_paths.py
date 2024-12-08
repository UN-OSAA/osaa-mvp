import os
from sqlmesh import macro
from sqlglot import exp

def parse_fully_qualified_name(fqtn):
    """Parse a fully qualified table name into its components."""
    # Convert SQLGlot expression to string
    if isinstance(fqtn, exp.Expression):
        if isinstance(fqtn, exp.Identifier):
            fqtn = fqtn.name
        else:
            fqtn = str(fqtn)
    
    parts = fqtn.split('.')
    if len(parts) != 3:
        raise ValueError("Fully qualified table name must be in the format 'database.schema.table'")
    database, schema, table = parts
    return database, schema, table

@macro()
def s3_landing_path(evaluator, subfolder_filename):
    """Construct S3 landing path."""
    bucket = os.environ.get('S3_BUCKET_NAME', 'osaa-mvp')
    target = os.environ.get('TARGET', 'prod').lower()
    username = os.environ.get('USERNAME', 'default').lower()

    # Construct the environment path segment
    env_path = target if target in ['prod', 'int'] else f"{target}_{username}"
    
    # Convert input to string if it's a SQLGlot expression
    if isinstance(subfolder_filename, exp.Expression):
        subfolder_filename = str(subfolder_filename).strip("'")
    
    path = f"s3://{bucket}/{env_path}/landing/{subfolder_filename}.parquet"
    return exp.Literal.string(path)

@macro()
def s3_transformed_path(evaluator, fqtn):
    """Construct S3 transformed path."""
    bucket = os.environ.get('S3_BUCKET_NAME', 'osaa-mvp')
    target = os.environ.get('TARGET', 'dev').lower()
    username = os.environ.get('USERNAME', 'default').lower()

    # Construct the environment path segment
    env_path = target if target in ['prod', 'int'] else f"{target}_{username}"
    
    
    _, schema, table = parse_fully_qualified_name(fqtn)
    table = table.strip("'")
    path = f"s3://{bucket}/{env_path}/transformed/{schema}/{table}"
    return exp.Literal.string(path)