# Serverless Promise.all() implementation using AWS services
## Requirements
- Python 3.12
- CDK 2.110 or higher (see [Getting Started with CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html))
- S3 bucket to store the tracker files (see `trackerBucketName` below)

## Usage
- Check out the code
- Open Terminal and navigate to the project folder
- Create a virtual environment: `python -m venv .venv`
- Activate the virtual environment: `source .venv/bin/activate`
- Install dependencies: `pip install -r requirements.txt`
- Create the bucket where you want to store the tracker files
- Deploy the stack: `cdk deploy -c trackerBucketName=<your-bucket-name>`
- Use the AWS console to invoke the function. Example payload that will generate 10 SQS messages for task-worker lambda:
```json
{
  "batch_size": 10
}
```

## How it works
- user invokes task-generator lambda with payload containing the number of tasks to generate
- task-generator sends all these tasks to SQS task-queue.
- task-worker lambda listens task-queue and processes the tasks in parallel.
- task-worker sleeps for random period of time between 1 and 20 seconds to simulate async task completion.
- each successful task-worker lambda execution creates the SQS message in SQS task-done-queue.
- task-tracker lambda listens task-done-queue and keeps track of the tasks that are completed.
- once all the tasks are completed, task-tracker lambda sends SQS all-tasks-done-queue.
- tracker files will be stored under `task-done-tracker` folder in the S3 bucket you specified during deployment.