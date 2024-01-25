from datetime import datetime
from time import sleep
import boto3
import os
import json
import random
from concurrent.futures import ThreadPoolExecutor

ITEM_QUEUE_URL = os.environ['ITEM_QUEUE_URL']
ITEM_BUCKET_NAME = os.environ['ITEM_BUCKET_NAME']

sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')


def handler(event, context):
    print(event)
    task = json.loads(event['Records'][0]['body'])
    rate_limit = task['rateLimit']
    delay_seconds = task['delaySeconds']
    items_to_load = task['items'][0:rate_limit]

    with ThreadPoolExecutor(max_workers=rate_limit) as executor:
        results = executor.map(get_item, [item['item_id'] for item in items_to_load])

    today = datetime.now().strftime('%Y-%m-%d')

    for item in results:
        item_key = f'item/{today}/{item["id"]}.json'
        s3_client.put_object(
            Bucket=ITEM_BUCKET_NAME,
            Key=item_key,
            Body=json.dumps(item)
        )

    new_task = {
        'rateLimit': rate_limit,
        'delaySeconds': delay_seconds,
        'items': task['items'][rate_limit:]
    }
    if len(new_task['items']) > 0:
        sqs_client.send_message(
            QueueUrl=ITEM_QUEUE_URL,
            MessageBody=json.dumps(new_task),
            DelaySeconds=delay_seconds
        )


def get_item(item_id):
    print('api call')
    sleep_time = random.randint(100, 1000)
    sleep(sleep_time/1000)
    return {'id': item_id, 'name': f'Item {item_id}'}
