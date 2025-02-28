import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh import model
from macros.ibis_expressions import generate_ibis_table
from typing import Optional, Dict, Union, Any

COLUMN_SCHEMA = {'country_id': 'String', 'indicator_id': 'String', 'year': 'Int', 'value': 'Decimal',
'magnitude': 'String', 'qualifier': 'String', 'indicator_description': 'String', 'avg_value_by_country': 'Float'} 

WDI_COLUMN_SCHEMA = {'country_id': 'String', 'indicator_id': 'String', 'year': 'Int', 'value': 'Decimal',
'magnitude': 'String', 'qualifier': 'String', 'indicator_description': 'String'}

def entrypoint(evaluator: MacroEvaluator):
    try:
        wdi = generate_ibis_table(evaluator, table_name='wdi', schema_name=
            'sources', column_schema=WDI_COLUMN_SCHEMA)
        
        # Now that generate_ibis_table always returns an ibis Table object, we don't need to check for strings
        country_averages = wdi.filter(wdi.value.notnull()).group_by([
            'country_id', 'indicator_id']).agg(avg_value_by_country=wdi.value.
            mean()).join(wdi, ['country_id', 'indicator_id']).select('country_id',
            'indicator_id', 'year', 'value', 'magnitude', 'qualifier',
            'indicator_description', 'avg_value_by_country')
            
        return ibis.to_sql(country_averages)
    except Exception as e:
        print(f'Error processing WDI country averages: {str(e)}')
        # Create a SQL query that returns an empty result set with the correct schema
        columns = []
        for col_name, col_type in COLUMN_SCHEMA.items():
            if col_type in ['String']:
                sql_type = 'VARCHAR'
            elif col_type in ['Int', 'Int64']:
                sql_type = 'INTEGER'
            elif col_type in ['Decimal']:
                sql_type = 'DECIMAL(18,3)'
            elif col_type in ['Float']:
                sql_type = 'FLOAT'
            else:
                sql_type = 'VARCHAR'
            columns.append(f'CAST(NULL AS {sql_type}) AS {col_name}')
        query = f"SELECT {', '.join(columns)} WHERE 1=0"
        return query 