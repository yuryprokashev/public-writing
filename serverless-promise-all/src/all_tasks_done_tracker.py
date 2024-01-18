import json
import os
import boto3
ALL_TASKS_DONE_QUEUE_URL = os.getenv('ALL_TASKS_DONE_QUEUE_URL')
TRACKER_BUCKET_NAME = os.getenv('TRACKER_BUCKET_NAME')


def handler(event, context):
    print('tracker bucket name', TRACKER_BUCKET_NAME)
    print(event)
    # extract record bodies from event records and parse them
    records = [json.loads(record['body']) for record in event['Records']]
    print(records[0])
    batch_id = records[0]['batch_id']
    batch_size = records[0]['batch_size']

    # read the current counter from tracker bucket (use task-done-tracker/{batch_id} as key)
    s3 = boto3.client('s3')
    tracker_key = f'task-done-tracker/{batch_id}.json'
    print('tracker key', tracker_key)
    try:
        tracker_object = s3.get_object(Bucket=TRACKER_BUCKET_NAME, Key=tracker_key)
        tracker_body = tracker_object['Body'].read().decode('utf-8')
        tracker_object = json.loads(tracker_body)
    except s3.exceptions.NoSuchKey:
        tracker_object = {'count': 0}
    except Exception as e:
        raise e
    tracker_count = tracker_object['count']
    print(tracker_count)
    # increment the counter by batch_size
    tracker_count += len(records)

    # if counter is equal to batch_size, send message to all_tasks_done_queue
    if tracker_count == batch_size:
        sqs = boto3.client('sqs')
        sqs.send_message(
            QueueUrl=ALL_TASKS_DONE_QUEUE_URL,
            MessageBody=json.dumps({'batch_id': batch_id})
        )

    # write the updated counter to tracker bucket
    new_tracker = {'count': tracker_count}
    s3.put_object(
        Bucket=TRACKER_BUCKET_NAME,
        Key=tracker_key,
        Body=json.dumps(new_tracker)
    )
    print('tracker count updated')

