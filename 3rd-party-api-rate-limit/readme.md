# How to load data from the third-party API with rate limit?
## Requirements
- Python 3.12
- CDK 2.110 or higher (see [Getting Started with CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html))
- S3 bucket to store the item details files (see `itemBucketName` below)

## Usage
- Check out the code
- Open Terminal and navigate to the project folder
- Create a virtual environment: `python -m venv .venv`
- Activate the virtual environment: `source .venv/bin/activate`
- Install dependencies: `pip install -r requirements.txt`
- Create the bucket where you want to store the loaded item files
- Deploy the stack: `cdk deploy -c itemBucketName=<your-bucket-name>`
- Use the AWS console to invoke the function.


## How it works
- user invokes generator lambda. Any payload is ok, since generator doesn't use it.
- generator simulates api call to GET /items and sends a single load task to the SQS queue (item-queue) with all item ids to fetch.
- item-loader lambda listens item-queue.
- item-loader lambda slices a chunk of the items, the chunk size is based on `rateLimit` parameter.
- item-loader fetches chunk items from the API in parallel and stores them in the S3 bucket.
- item-loader lambda packs the rest of the items into the next task if there are still items to fetch.
- item-loader lambda sends the next task to the item-queue with delay provided in `delaySeconds` parameter.

## Load Task Example
```json
{
  "rateLimit": 100,
  "delaySeconds": 60,
  "items": [
    {
      "item_id": "0"
    },
    ...
    {
      "item_id": "999"
    }
  ]
}
```