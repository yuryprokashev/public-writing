import json
import os
import time
import traceback
import jsonschema

import boto3
import pandas as pd

athena_client = boto3.client('athena')
DATABASE_NAME = os.environ.get('DATABASE_NAME', None)
WORKGROUP_NAME = os.environ.get('WORKGROUP_NAME', None)
daily_returns_input_schema = {
    'type': 'object',
    'properties': {
        'date': {'type': 'string', 'pattern': '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'}
    },
    'required': ['date']
}
daily_returns_query = '''
WITH DailyReturnsInput AS (
    SELECT 
        symbol, 
        timestamp,
        close,
        lag(close) over (PARTITION by symbol order by timestamp) as previous_close
    FROM 
        candle
),
DailyReturns AS (
    SELECT 
        symbol,
        timestamp,
        (close - previous_close) / previous_close as daily_return
    FROM 
        DailyReturnsInput
)
select * from DailyReturns
where timestamp = cast(? as timestamp)
'''


def main(event, context):
    print(event)
    # Step 1. Extract and validate input from request query string
    try:
        query_string_params = event.get('queryStringParameters', None)
        if query_string_params is None:
            raise Exception('query string is required')
        jsonschema.validate(query_string_params, daily_returns_input_schema)
    except Exception as e:
        return create_api_error(400, e)

    # Step 2. Query Athena and return results
    try:
        date = query_string_params.get('date')
        daily_returns_athena_out = get_athena_results(
            athena_client=athena_client,
            database_name=DATABASE_NAME,
            query=daily_returns_query,
            workgroup=WORKGROUP_NAME,
            waiting_time_ms=100,
            parameters=[f'\'{date}\''],
        )
        athena_results_as_df = athena_results_to_df(daily_returns_athena_out)
        athena_results_as_json = athena_results_as_df.to_json(orient='records')
        return create_api_response(athena_results_as_json)
    except Exception as e:
        return create_api_error(500, e)


def create_api_response(payload):
    return {
        'statusCode': 200,
        'body': payload
    }


def create_api_error(code, exception):
    error = {
        'message': str(exception),
        'stack': traceback.format_exc()
    }
    return {
        'statusCode': code,
        'body': json.dumps(error)
    }


def get_athena_results(
        athena_client,
        database_name,
        query,
        parameters=None,
        workgroup='primary',
        catalog_id='AwsDataCatalog',
        waiting_time_ms=100,
        debug=False
):
    if debug:
        print(f'Query: {query}')
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Catalog': catalog_id,
            'Database': database_name
        },
        WorkGroup=workgroup,
        ExecutionParameters=parameters
    )
    query_execution_id = response['QueryExecutionId']
    print(f'QueryExecutionId: {query_execution_id}')
    query_status = 'RUNNING'
    while query_status in ['RUNNING', 'QUEUED']:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        query_status = response['QueryExecution']['Status']['State']
        print(f'Query status: {query_status}')
        if query_status in ['RUNNING', 'QUEUED']:
            time.sleep(waiting_time_ms / 1000)
    if query_status != 'SUCCEEDED':
        reason = response['QueryExecution']['Status']['StateChangeReason']
        raise Exception(f'Query status: {query_status}\n{reason}')

    response_iterator = athena_client.get_paginator('get_query_results').paginate(
        QueryExecutionId=query_execution_id
    )
    raw_rows = []
    column_info = None
    for page in response_iterator:
        if not column_info:
            column_info = page['ResultSet']['ResultSetMetadata']['ColumnInfo']
        raw_rows.extend(page['ResultSet']['Rows'])
    response = {
        'ResultSet': {
            'ResultSetMetadata': {
                'ColumnInfo': column_info
            },
            'Rows': raw_rows
        }
    }
    return response


def get_athena_results_with_reuse(
        athena_client,
        database_name,
        query,
        parameters=None,
        workgroup='primary',
        catalog_id='AwsDataCatalog',
        waiting_time_ms=100,
        max_age_in_minutes=1,
        debug=False
):
    if debug:
        print(f'Query: {query}')
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Catalog': catalog_id,
            'Database': database_name
        },
        WorkGroup=workgroup,
        ResultReuseConfiguration={
            'ResultReuseByAgeConfiguration': {
                'Enabled': True,
                'MaxAgeInMinutes': max_age_in_minutes
            }
        },
        ExecutionParameters=parameters
    )
    query_execution_id = response['QueryExecutionId']
    print(f'QueryExecutionId: {query_execution_id}')
    query_status = 'RUNNING'
    while query_status in ['RUNNING', 'QUEUED']:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        query_status = response['QueryExecution']['Status']['State']
        print(f'Query status: {query_status}')
        if query_status in ['RUNNING', 'QUEUED']:
            time.sleep(waiting_time_ms / 1000)
    if query_status != 'SUCCEEDED':
        reason = response['QueryExecution']['Status']['StateChangeReason']
        raise Exception(f'Query status: {query_status}\n{reason}')

    response_iterator = athena_client.get_paginator('get_query_results').paginate(
        QueryExecutionId=query_execution_id
    )
    raw_rows = []
    column_info = None
    for page in response_iterator:
        if not column_info:
            column_info = page['ResultSet']['ResultSetMetadata']['ColumnInfo']
        raw_rows.extend(page['ResultSet']['Rows'])
    response = {
        'ResultSet': {
            'ResultSetMetadata': {
                'ColumnInfo': column_info
            },
            'Rows': raw_rows
        }
    }
    return response


def athena_results_to_df(athena_results) -> pd.DataFrame:
    columns = [col['Name'] for col in athena_results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    column_types = [col['Type'] for col in athena_results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    rows = athena_results['ResultSet']['Rows'][1:]
    data_typed = []
    for i, row in enumerate(rows):
        data_typed.append(tuple(
            athena_value_parser(column_type, value.get('VarCharValue', None))
            for column_type, value in zip(column_types, row['Data'])
        ))

    return pd.DataFrame(data_typed, columns=columns)


def athena_value_parser(column_type, value):
    if value is None:
        return None
    if column_type in ["varchar", "timestamp", "char", "string", "date", "array"]:
        return value
    if column_type in ["int", "bigint", "integer", "smallint", "tinyint"]:
        return int(value)
    if column_type in ["double", "float", "decimal"]:
        return float(value)
    if column_type in ["boolean"]:
        return value.lower() == "true"
    raise Exception(f"Unknown Athena column type: {column_type}")

