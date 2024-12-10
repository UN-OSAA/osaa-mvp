MODEL (
    name wdi.series,
    kind FULL
  );

  SELECT
    *
  FROM
    read_parquet(
        @s3_landing_path('wdi/WDISeries')
    )
