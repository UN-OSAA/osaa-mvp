"""Module for processing World Development Indicators (WDI) data.

This module provides functionality to transform and process World Development
Indicators data using SQLMesh and Ibis, preparing it for further analysis.

The module focuses on pivoting and transforming WDI data, converting time-series
data into a more analysis-friendly format with country codes, indicator codes,
years, and values.
"""

import typing as t
from datetime import datetime

import ibis  # type: ignore
import ibis.selectors as s  # type: ignore
import pandas as pd  # type: ignore
from sqlmesh import ExecutionContext, model  # type: ignore


@model(
    "intermediate.wdi",
    kind="FULL",
    columns={
        "country_id": "text",
        "indicator_id": "text",
        "year": "int",
        "value": "text",
        "indicator_label": "text",
    },
    start=model.Field(default=datetime(2000, 1, 1)),  # type: ignore
    end=model.Field(default=datetime(2022, 12, 31)),  # type: ignore
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    """Process WDI data and return the transformed Ibis table.

    Args:
        context: SQLMesh execution context
        start: Start date for data processing
        end: End date for data processing
        execution_time: Timestamp of execution
        **kwargs: Additional keyword arguments

    Returns:
        Processed WDI data as a pandas DataFrame
    """
    print("Starting wdi_data")
    wdi_table = context.table("wdi.csv")
    wdi_df = context.fetchdf(wdi_table.select("*"))  # type: ignore

    wdi_data = (
        ibis.memtable(wdi_df, name="wdi")
        .rename("snake_case")
        .pivot_longer(s.r["1960":], names_to="year", values_to="value")  # type: ignore
        .cast({"year": "int64"})
        .rename(country_id="country_code", indicator_id="indicator_code")
    )

    print("Starting wdi_label")
    wdi_label_table = context.table("wdi.series")
    context.fetchdf(wdi_label_table.select("*"))  # type: ignore

    return wdi_data.to_pandas()
