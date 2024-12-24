MODEL (
    name wdi.series,
    kind FULL,
    columns (
      "Series Code" TEXT,
      "Topic" TEXT,
      "Indicator Name" TEXT,
      "Short definition" TEXT,
      "Long definition" TEXT,
      "Unit of measure" TEXT,
      "Periodicity" TEXT,
      "Base Period" TEXT,
      "Other notes" TEXT,
      "Aggregation method" TEXT,
      "Limitations and exceptions" TEXT,
      "Notes from original source" TEXT,
      "General comments" TEXT,
      "Source" TEXT,
      "Statistical concept and methodology" TEXT,
      "Development relevance" TEXT,
      "Related source links" TEXT,
      "Other web links" TEXT,
      "Related indicators" TEXT,
      "License Type" TEXT
    )

  );

  SELECT
    *
  FROM
    read_parquet(
        @s3_landing_path('wdi/WDISeries')
    )
