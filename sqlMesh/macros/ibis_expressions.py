from sqlmesh import macro
from ibis.expr.operations import Namespace, UnboundTable


@macro()
def generate_ibis_table(
    evaluator,
    table_name: str,
    schema_name: str,
    column_schema: dict,
    catalog_name: str = "osaa_mvp",
):
    """
    This macro generates an ibis table expression based on the provided parameters.
    Note: For DuckDB, the "database" parameter in Ibis's "Namespace" class corresponds to the table schema, hence schema_name is used for "database".
    """
    try:
        if not table_name or not schema_name or not column_schema:
            raise ValueError(
                "table_name, schema_name, and column_schema are required parameters"
            )

        table = UnboundTable(
            name=table_name,
            schema=column_schema,
            namespace=Namespace(catalog=catalog_name, database=schema_name),
        ).to_expr()

        return table
    except Exception as e:
        raise RuntimeError(f"Failed to create ibis table: {str(e)}")
