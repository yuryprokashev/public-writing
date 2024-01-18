import os
import boto3

TASK_DONE_QUEUE_URL = os.getenv('TASK_DONE_QUEUE_URL')


def handler(event, context):
    print(event)
