def main(event, context):
    print(event)

    return {
        'isAuthorized': True,
        'resolverContext': {
            'roses': 'red'
        }
    }
