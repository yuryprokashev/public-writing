import os


def main(event, context):
    print(event)
    with open('index.html', 'r') as file:
        html_body = file.read()

    with open('frontend/dist/bundle.js', 'r') as file:
        script_dist = file.read()

    html_body = html_body.replace('{{SCRIPT_DIST}}', script_dist)

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/html'
        },
        'body': html_body
    }
