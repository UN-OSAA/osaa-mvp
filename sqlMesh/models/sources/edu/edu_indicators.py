"""Module for processing educational data from various datasets.

This module provides functionality to process and transform educational data
from different sources such as OPRI and SDG.
"""

import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh import model
import logging
from typing import Optional
from constants import DB_PATH

@model(
    "sources.edu",
    is_sql=True,
    kind="FULL",
    columns={
        "country_id": "String",
        "indicator_id": "String",
        "year": "Int",
        "value": "Decimal",
        "indicator_description": "String",
    }
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Process educational data and return the transformed Ibis table."""
    try:
        # Connect to DuckDB
        con = ibis.connect(f"duckdb://{DB_PATH}")
        
        # Try to process educational data from different sources
        source_tables = []
        
        # Try to process OPRI edu data
        try:
            opri_data = con.table("opri.data_national")
            opri_label = con.table("opri.label")
            
            opri_edu = (
                opri_data.join(opri_label, "indicator_id", how="left")
                .select(
                    "country_id",
                    "indicator_id",
                    "year",
                    "value",
                    indicator_description="indicator_label_en",
                )
                .filter(opri_data.year > 1999)
            )
            
            source_tables.append(opri_edu)
        except Exception as e:
            print(f"Warning: Could not process OPRI educational data: {e}")
            
        # Try to process SDG edu data
        try:
            sdg_data = con.table("sdg.data_national")
            sdg_label = con.table("sdg.label")
            
            sdg_edu = (
                sdg_data.join(sdg_label, "indicator_id", how="left")
                .select(
                    "country_id",
                    "indicator_id",
                    "year",
                    "value",
                    indicator_description="indicator_label_en",
                )
                .filter(sdg_data.year > 1999)
            )
            
            source_tables.append(sdg_edu)
        except Exception as e:
            print(f"Warning: Could not process SDG educational data: {e}")
            
        # Union all edu tables if we have any
        if not source_tables:
            # Return an empty result set with the correct schema
            return create_empty_result()
            
        unioned_t = ibis.union(*source_tables)
        return ibis.to_sql(unioned_t)
        
    except Exception as e:
        # If any error occurs, return an empty table with the right schema
        print(f"Error processing educational data: {e}")
        return create_empty_result()

def create_empty_result():
    """Create an empty SQL result with the correct schema."""
    return """
    SELECT 
        '' AS country_id,
        '' AS indicator_id,
        0 AS year,
        0.0 AS value,
        '' AS indicator_description
    WHERE 1=0
    """ 