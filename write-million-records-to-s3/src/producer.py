import json
import os
from datetime import datetime
import random
from functools import partial

import boto3
import pandas as pd

s3_client = boto3.client('s3')
fh_client = boto3.client('firehose')
BUCKET_NAME = os.environ['BUCKET_NAME']
FIREHOSE_NAME = os.environ['FIREHOSE_NAME']


def write_json_collection(client, records, bucket_name):
    if not bucket_name:
        raise ValueError('bucket_name is not provided')
    file_path = '/tmp/records.json'
    with open(file_path, 'w') as file:
        for record in records:
            file.write(json.dumps(record) + '\n')
    client.upload_file(
        file_path,
        bucket_name,
        'glue-db/json-data-collection/records.json'
    )
    return {
        'FailedPutCount': 0,
    }


def write_parquet(client, records, bucket_name):
    if not bucket_name:
        raise ValueError('bucket_name is not provided')
    df = pd.DataFrame(records)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    file_path = '/tmp/output.parquet'
    df.to_parquet(path=file_path, compression='snappy')
    client.upload_file(
        file_path,
        bucket_name,
        'glue-db/parquet-data/output.parquet'
    )
    return {
        'FailedPutCount': 0,
    }


def write_kinesis_firehose(client, records, firehose_name):
    print('firehose_name', firehose_name)
    if not firehose_name:
        raise ValueError('firehose_name is not provided')

    # Append a newline character to each JSON record
    _input = [json.dumps(record) + '\n' for record in records]

    batch_size = 500  # Max batch size for Firehose
    response = {'FailedPutCount': 0, 'RequestResponses': []}
    # Split the input into batches of 500 and send each batch to Firehose
    for i in range(0, len(_input), batch_size):
        # Convert string data to bytes
        batch_records = [
            {'Data': record.encode()} for record in _input[i:i + batch_size]
        ]
        out = client.put_record_batch(
            DeliveryStreamName=firehose_name,
            Records=batch_records
        )
        response['FailedPutCount'] += out['FailedPutCount']
        response['RequestResponses'].extend(out['RequestResponses'])
    return response


writer_map = {
    'json': partial(write_json_collection, bucket_name=BUCKET_NAME, client=s3_client),
    'parquet': partial(write_parquet, bucket_name=BUCKET_NAME, client=s3_client),
    'kinesis_firehose': partial(write_kinesis_firehose, firehose_name=FIREHOSE_NAME, client=fh_client)
}


def handler(event, context):
    count = event.get('count', 1000)
    writer_name = event.get('writer', 'json')
    if writer_name not in writer_map:
        raise ValueError(f'writer \'{writer_name}\' not supported')
    writer = writer_map[writer_name]
    gen_records_start = datetime.now()
    records = generate_records(count=count)
    gen_records_end = datetime.now()

    writer_out = writer(records=records)
    if 'FailedPutCount' in writer_out:
        print('FailedPutCount', writer_out['FailedPutCount'])
    else:
        print('writer_out', writer_out)
    write_records_end = datetime.now()
    print(f'Generated {count} records in {(gen_records_end - gen_records_start)}')
    print(f'\'{writer_name}\' wrote {count} records in {(write_records_end - gen_records_end)}')


def generate_records(count):
    records = []
    for i in range(count):
        record = {
            'timestamp': generate_timestamp(1650000000, 1700000000),
            'symbol_key': random.choice(['AAPL', 'AMZN', 'GOOG', 'MSFT', 'META']),
            'parameter_key': random.choice(['open', 'high', 'low', 'close', 'volume']),
            'parameter_value': random.uniform(100, 1000)
        }
        records.append(record)
    return records


def generate_timestamp(start_at, end_at):
    ts = random.randint(start_at, end_at)
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S.%f")

