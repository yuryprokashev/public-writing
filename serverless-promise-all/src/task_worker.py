import json
import os
import boto3
import random
import time

TASK_DONE_QUEUE_URL = os.getenv('TASK_DONE_QUEUE_URL')


def handler(event, context):
    print(event)
    # create sqs client
    sqs = boto3.client('sqs')
    # extract body from the first record and parse it
    record = json.loads(event['Records'][0]['body'])
    # random sleep for 1-20 seconds
    sleep_time = random.randint(1, 20)
    print(f'sleeping for {sleep_time} seconds')
    time.sleep(sleep_time)
    print('done', record)
    # send message to task_done_queue
    sqs.send_message(
        QueueUrl=TASK_DONE_QUEUE_URL,
        MessageBody=json.dumps(record)
    )
