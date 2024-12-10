MODEL (
    name wdi.csv,
    kind FULL
  );

  SELECT
    *
  FROM
      read_parquet(
        @s3_landing_path('wdi/WDICSV')
    )
