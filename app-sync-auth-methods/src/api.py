def start_process(event, context):
    print(event)
    return {
        'id': event.get('arguments', {}).get('process_id', 'unknown'),
        'status': 'PENDING'
    }


def get_process(event, context):
    print(event)
    return {
        'id': event.get('arguments', {}).get('process_id', 'unknown'),
        'status': 'COMPLETED'
    }
