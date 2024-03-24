import json
import os
import time

import boto3
import random


SOURCE = os.environ.get('SOURCE', None)

event_bridge = boto3.client('events')


def main(event, context):
    print(event)
    time_to_sleep = random.randint(5, 10)
    time.sleep(time_to_sleep)
    process_id = event['detail']['id']
    event_bridge.put_events(
        Entries=[
            {
                'Source': SOURCE,
                'DetailType': 'async-process-event',
                'Detail': json.dumps({
                    'status': 'done',
                    'id': process_id
                })
            }
        ]
    )



