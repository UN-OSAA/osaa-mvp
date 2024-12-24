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


@model(
    "intermediate.sdg",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    description="""
    This model contains Sustainable Development Goals (SDG) data for all countries and indicators.
    Publishing Org: UN
    Link to raw data: https://unstats.un.org/sdgs/dataportal
    Dataset owner:
        Name: UN
        Contact info: https://unstats.un.org/sdgs/contact-us/
    Funding Source: 
    Maintenance Status: Actively Maintained
    How data was collected: https://unstats.un.org/sdgs/dataContacts/
    Update Cadence: Annually
    Transformations of raw data (column renaming, type casting, etc): indicator labels and descriptions are joined together
    Column descriptions:
        indicator_id: The unique identifier for the indicator
        country_id: The unique identifier for the country
        year: The year of the data
        value: The value of the indicator for the country and year
        magnitude: The magnitude of the indicator for the country and year
        qualifier: The qualifier of the indicator for the country and year
        indicator_description: The description of the indicator
    Primary Key: indicator_id, country_id, year
    """,
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    source_folder_path = "sources/sdg"

    opri_data_national = generate_ibis_table(
        evaluator,
        table_name="data_national",
        column_schema=get_sql_model_schema("data_national", source_folder_path),
        schema_name="sdg",
    )

    opri_label = generate_ibis_table(
        evaluator,
        table_name="label",
        column_schema=get_sql_model_schema("label", source_folder_path),
        schema_name="sdg",
    )

    int_sdg = (
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

    return ibis.to_sql(int_sdg)
