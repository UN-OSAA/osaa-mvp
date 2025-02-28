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
    wdi = generate_ibis_table(evaluator, table_name='wdi', schema_name=
        'sources', column_schema=WDI_COLUMN_SCHEMA)
    
    country_averages = wdi.filter(wdi.value.notnull()).group_by([
        'country_id', 'indicator_id']).agg(avg_value_by_country=wdi.value.
        mean()).join(wdi, ['country_id', 'indicator_id']).select('country_id',
        'indicator_id', 'year', 'value', 'magnitude', 'qualifier',
        'indicator_description', 'avg_value_by_country')
        
    return ibis.to_sql(country_averages) 