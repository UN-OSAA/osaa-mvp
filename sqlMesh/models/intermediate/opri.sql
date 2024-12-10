MODEL (
  name intermediate.opri,
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
FROM opri.data_national AS series
LEFT JOIN opri.label AS label
  ON series.indicator_id = label.indicator_id;
