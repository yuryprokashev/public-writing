import os
import time

import boto3
import json
TASK_QUEUE_URL = os.getenv('TASK_QUEUE_URL')


def handler(event, context):
    # create sqs client
    sqs = boto3.client('sqs')
    # generate tasks with batch_id as timestamp in microseconds and batch_size as batch_size in event
    batch_id = str(int(time.time() * 1000000))
    batch_size = event['batch_size'] if 'batch_size' in event else 2
    tasks = [{'batch_id': batch_id, 'batch_size': batch_size, 'task_id': i} for i in range(batch_size)]

    # split tasks to batches of 10 and send batches to sqs queue using send_message_batch
    for i in range(0, len(tasks), 10):
        batch = tasks[i:i+10]
        sqs.send_message_batch(
            QueueUrl=TASK_QUEUE_URL,
            Entries=[
                {
                    'Id': str(i),
                    'MessageBody': json.dumps(task)
                } for i, task in enumerate(batch)
            ]
        )
