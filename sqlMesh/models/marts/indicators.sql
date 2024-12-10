MODEL (
  name marts.indicators,
  kind FULL
);

WITH sdg_data AS (
  SELECT
    *
  FROM intermediate.sdg
), opri_data AS (
  SELECT
    *
  FROM intermediate.opri
), unioned AS (
  SELECT
    *,
    'sdg' AS source
  FROM sdg_data
  UNION ALL
  SELECT
    *,
    'opri' AS source
  FROM opri_data
)
SELECT
  indicator_id::TEXT,
  country_id::TEXT,
  year::INT,
  value::DECIMAL(18, 3),
  magnitude::TEXT,
  qualifier::TEXT,
  indicator_description::TEXT,
  source::TEXT
FROM unioned
ORDER BY
  year,
  country_id,
  indicator_id;
