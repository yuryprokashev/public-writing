import json
import http.client
from urllib.parse import urlparse

import boto3
import os

SOURCE = os.environ.get('SOURCE', None)
API_URL = os.environ.get('API_URL', None)
API_HOST = urlparse(API_URL).netloc
API_KEY = os.environ.get('API_KEY', None)

event_bridge = boto3.client('events')


def start_process(event, context):
    print(event)
    process_id = event.get('arguments', {}).get('process_id', None)
    if process_id is None:
        raise ValueError('process_id is required')

    event_bridge.put_events(
        Entries=[
            {
                'Source': SOURCE,
                'DetailType': 'async-process-event',
                'Detail': json.dumps(
                    {
                        'status': 'start',
                        'id': process_id
                    }
                )
            }
        ]
    )
    return {
        'id': process_id,
        'status': 'PENDING'
    }


def end_process(event, context):
    print(event)
    return {
        'id': event.get('arguments', {}).get('process_id', None),
        'status': 'DONE'
    }


def async_process_listener(event, context):
    print(event)
    query = '''
    mutation m ($process_id: String!) {
      endProcess(process_id: $process_id) {
        id
        status
      }
    }
    '''
    variables = {
        'process_id': event['detail']['id']
    }
    headers = {'x-api-key': API_KEY, 'Content-Type': 'application/json'}
    connection = http.client.HTTPSConnection(API_HOST)
    connection.request('POST', '/graphql', json.dumps({'query': query, 'variables': variables}), headers)
    response = connection.getresponse()
    data = json.loads(
        response.read().decode()
    )
    errors = data.get('errors', None)
    if errors:
        raise ValueError(f'Error: {data}')
