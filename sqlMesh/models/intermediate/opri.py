import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.ibis_expressions import generate_ibis_table
from macros.utils import get_sql_model_schema

COLUMN_SCHEMA = {
    "indicator_id": "String",
    "country_id": "String",
    "year": "Int",
    "value": "Decimal",
    "magnitude": "String",
    "qualifier": "String",
    "indicator_description": "String",
}


@model("intermediate.opri", is_sql=True, kind="FULL", columns=COLUMN_SCHEMA)
def entrypoint(evaluator: MacroEvaluator) -> str:
    source_folder_path = "sources/opri"

    opri_data_national = generate_ibis_table(
        evaluator,
        table_name="data_national",
        column_schema=get_sql_model_schema("data_national", source_folder_path),
        schema_name="opri",
    )

    opri_label = generate_ibis_table(
        evaluator,
        table_name="label",
        column_schema=get_sql_model_schema("label", source_folder_path),
        schema_name="opri",
    )

    int_opri = (
        opri_data_national.left_join(opri_label, "indicator_id")
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

    return ibis.to_sql(int_opri)
