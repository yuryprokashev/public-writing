from time import sleep

import boto3
import os
import json
ITEM_QUEUE_URL = os.environ['ITEM_QUEUE_URL']
sqs_client = boto3.client('sqs')


def handler(event, context):
    items = get_items()
    mapped_items = []
    for item in items:
        mapped_items.append({'item_id': item['id']})
    task = {
        'rateLimit': 100,
        'delaySeconds': 60,
        'items': mapped_items
    }
    sqs_client.send_message(
        QueueUrl=ITEM_QUEUE_URL,
        MessageBody=json.dumps(task),
        DelaySeconds=60
    )


def get_items():
    print('api call')
    result = []
    for i in range(1000):
        result.append({'id': i})
    sleep(1)
    return result
