import json
import os
import boto3
ALL_TASKS_DONE_QUEUE_URL = os.getenv('ALL_TASKS_DONE_QUEUE_URL')


def handler(event, context):
    print(event)
#     extract record bodies
    records = [json.load(record['body']) for record in event['Records']]
    print(records)

