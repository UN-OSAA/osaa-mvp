MODEL (
    name intermediate.wdi_country_averages,
    kind FULL
);

WITH source_data AS (
    SELECT *
    FROM intermediate.wdi
),

country_averages AS (
    SELECT
        country_id,
        AVG(CAST(value AS FLOAT)) as avg_value_by_country
    FROM source_data
    WHERE value IS NOT NULL
      AND value != ''
      AND TRY_CAST(value AS FLOAT) IS NOT NULL
    GROUP BY country_id
),

final AS (
    SELECT
        s.*,
        ca.avg_value_by_country
    FROM source_data s
    LEFT JOIN country_averages ca ON s.country_id = ca.country_id
)

SELECT * FROM final
;

@IF(
  @runtime_stage = 'evaluating',
    COPY (SELECT * FROM intermediate.wdi_country_averages)
    TO @s3_transformed_path('osaa_mvp.intermediate.wdi_country_averages')
    (FORMAT PARQUET)
)
;
