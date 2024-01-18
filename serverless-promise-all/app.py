#!/usr/bin/env python3
import aws_cdk as cdk
from constructs import Construct


class MyStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        task_done_queue, all_tasks_done_queue = self.promise_all_implementation()
        self.task_processor(task_done_queue)
        self.next_process(all_tasks_done_queue)

    def promise_all_implementation(self) -> tuple:
        tracker_bucket_name = self.node.get_context('trackerBucketName')
        tracker_bucket = cdk.aws_s3.Bucket.from_bucket_name(self, 'tracker-bucket', tracker_bucket_name)
        lambda_timeout = cdk.Duration.seconds(60)
        queue_visibility_timeout = cdk.Duration.seconds(90)

        task_done_queue = cdk.aws_sqs.Queue(self, 'task-done-queue', visibility_timeout=queue_visibility_timeout)

        all_tasks_done_queue = cdk.aws_sqs.Queue(self, 'all-tasks-done-queue', visibility_timeout=queue_visibility_timeout)

        all_tasks_done_tracker = cdk.aws_lambda.Function(
            self, 'allTasksDoneTracker',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='all_tasks_done_tracker.handler',
            code=cdk.aws_lambda.Code.from_asset('./src'),
            timeout=lambda_timeout,
            memory_size=256,
            architecture=cdk.aws_lambda.Architecture.ARM_64,
            environment={
                'TRACKER_BUCKET_NAME': tracker_bucket_name,
                'ALL_TASKS_DONE_QUEUE_URL': all_tasks_done_queue.queue_url
            }
        )
        all_tasks_done_tracker.add_to_role_policy(
            cdk.aws_iam.PolicyStatement(
                actions=['s3:PutObject', 's3:GetObject'],
                resources=[tracker_bucket.bucket_arn + '/*']
            )
        )
        all_tasks_done_tracker.add_to_role_policy(
            cdk.aws_iam.PolicyStatement(
                actions=['sqs:ReceiveMessage', 'sqs:DeleteMessage'],
                resources=[task_done_queue.queue_arn]
            )
        )
        all_tasks_done_tracker.add_to_role_policy(
            cdk.aws_iam.PolicyStatement(
                actions=['sqs:SendMessage'],
                resources=[all_tasks_done_queue.queue_arn]
            )
        )
        event_source = cdk.aws_lambda_event_sources.SqsEventSource(
            queue=task_done_queue,
            batch_size=10,
            max_batching_window=cdk.Duration.seconds(10)
        )
        all_tasks_done_tracker.add_event_source(event_source)
        return task_done_queue, all_tasks_done_queue

    def task_processor(self, task_done_queue):
        lambda_timeout = cdk.Duration.seconds(60)
        queue_visibility_timeout = cdk.Duration.seconds(90)
        task_queue = cdk.aws_sqs.Queue(self, 'task-queue', visibility_timeout=queue_visibility_timeout)
        task_generator = cdk.aws_lambda.Function(
            self, 'task-generator',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='task_generator.handler',
            code=cdk.aws_lambda.Code.from_asset('./src'),
            timeout=lambda_timeout,
            memory_size=256,
            architecture=cdk.aws_lambda.Architecture.ARM_64,
            environment={
                'TASK_QUEUE_URL': task_queue.queue_url
            }
        )
        task_generator.add_to_role_policy(
            cdk.aws_iam.PolicyStatement(
                actions=['sqs:SendMessage'],
                resources=[task_queue.queue_arn]
            )
        )
        task_worker = cdk.aws_lambda.Function(
            self, 'task-worker',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='task_worker.handler',
            code=cdk.aws_lambda.Code.from_asset('./src'),
            timeout=lambda_timeout,
            memory_size=256,
            architecture=cdk.aws_lambda.Architecture.ARM_64,
            environment={
                'TASK_DONE_QUEUE_URL': task_done_queue.queue_url
            }
        )
        task_worker.add_to_role_policy(
            cdk.aws_iam.PolicyStatement(
                actions=['sqs:SendMessage'],
                resources=[task_done_queue.queue_arn]
            )
        )
        task_worker.add_to_role_policy(
            cdk.aws_iam.PolicyStatement(
                actions=['sqs:ReceiveMessage', 'sqs:DeleteMessage'],
                resources=[task_queue.queue_arn]
            )
        )
        task_source = cdk.aws_lambda_event_sources.SqsEventSource(
            queue=task_queue,
            batch_size=1,
        )
        task_worker.add_event_source(task_source)

    def next_process(self, all_tasks_done_queue):
        next_process_lambda = cdk.aws_lambda.Function(
            self, 'next-process',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='next_process.handler',
            code=cdk.aws_lambda.Code.from_asset('./src'),
            timeout=cdk.Duration.seconds(60),
            memory_size=256,
            architecture=cdk.aws_lambda.Architecture.ARM_64,
            reserved_concurrent_executions=1,
            environment={
                'ALL_TASKS_DONE_QUEUE_URL': all_tasks_done_queue.queue_url
            }
        )
        next_process_lambda.add_to_role_policy(
            cdk.aws_iam.PolicyStatement(
                actions=['sqs:ReceiveMessage', 'sqs:DeleteMessage'],
                resources=[all_tasks_done_queue.queue_arn]
            )
        )
        event_source = cdk.aws_lambda_event_sources.SqsEventSource(
            queue=all_tasks_done_queue,
            batch_size=1
        )
        next_process_lambda.add_event_source(event_source)


app = cdk.App()
MyStack(app, 'serverless-promise-all')
app.synth()
