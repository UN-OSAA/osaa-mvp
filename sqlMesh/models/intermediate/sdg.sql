MODEL (
  name intermediate.sdg,
  kind FULL
);

SELECT
  series.indicator_id::TEXT,
  series.country_id::TEXT,
  series.year::INT,
  series.value::DECIMAL(18, 3),
  series.magnitude::TEXT,
  series.qualifier::TEXT,
  label.indicator_label_en::TEXT AS indicator_description
FROM sdg.data_national AS series
LEFT JOIN sdg.label AS label
  ON series.indicator_id = label.indicator_id;
