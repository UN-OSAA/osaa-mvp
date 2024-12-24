MODEL (
    name opri.data_national,
    kind FULL,
    cron '@daily',
    columns (
      INDICATOR_ID TEXT,
      COUNTRY_ID TEXT,
      YEAR INTEGER,
      VALUE DECIMAL,
      MAGNITUDE TEXT,
      QUALIFIER TEXT
    )
  );

  SELECT
    *
  FROM

    read_parquet(
        @s3_landing_path('edu/OPRI_DATA_NATIONAL')
    )
