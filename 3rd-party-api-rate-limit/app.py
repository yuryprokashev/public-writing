#!/usr/bin/env python3
import aws_cdk as cdk
from constructs import Construct


class MyStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        item_bucket_name = self.node.get_context('itemBucketName')
        item_bucket = cdk.aws_s3.Bucket.from_bucket_name(self, 'item-bucket', item_bucket_name)
        item_loader_timeout = cdk.Duration.seconds(60)
        item_queue_visibility_timeout = cdk.Duration.seconds(90)
        dead_item_queue = cdk.aws_sqs.Queue(
            self, 'dead-item-queue'
        )
        item_queue = cdk.aws_sqs.Queue(
            self, 'item-queue',
            visibility_timeout=item_queue_visibility_timeout,
            dead_letter_queue=cdk.aws_sqs.DeadLetterQueue(
                queue=dead_item_queue,
                max_receive_count=3
            )
        )
        item_loader = cdk.aws_lambda.Function(
            self, 'item-loader',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='item_loader.handler',
            code=cdk.aws_lambda.Code.from_asset('./src'),
            timeout=item_loader_timeout,
            memory_size=256,
            architecture=cdk.aws_lambda.Architecture.ARM_64,
            reserved_concurrent_executions=1,
            environment={
                'ITEM_QUEUE_URL': item_queue.queue_url,
                'ITEM_BUCKET_NAME': item_bucket_name
            }
        )
        item_loader.add_to_role_policy(
            cdk.aws_iam.PolicyStatement(
                actions=['s3:PutObject', 's3:GetObject', 's3:ListBucket'],
                resources=[item_bucket.bucket_arn, f'{item_bucket.bucket_arn}/*']
            )
        )
        item_loader.add_to_role_policy(
            cdk.aws_iam.PolicyStatement(
                actions=['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:SendMessage'],
                resources=[item_queue.queue_arn]
            )
        )
        event_source = cdk.aws_lambda_event_sources.SqsEventSource(
            queue=item_queue,
            batch_size=1
        )
        item_loader.add_event_source(event_source)

        generator = cdk.aws_lambda.Function(
            self, 'generator',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='generator.handler',
            code=cdk.aws_lambda.Code.from_asset('./src'),
            timeout=item_loader_timeout,
            memory_size=256,
            architecture=cdk.aws_lambda.Architecture.ARM_64,
            reserved_concurrent_executions=1,
            environment={
                'ITEM_QUEUE_URL': item_queue.queue_url
            }
        )
        generator.add_to_role_policy(
            cdk.aws_iam.PolicyStatement(
                actions=['sqs:SendMessage'],
                resources=[item_queue.queue_arn]
            )
        )
        # invoke generator every day at 6am UTC
        cdk.aws_events.Rule(
            self, 'generator-schedule',
            enabled=False,
            schedule=cdk.aws_events.Schedule.cron(
                minute='0',
                hour='6',
                month='*',
                week_day='*',
                year='*'
            ),
            targets=[cdk.aws_events_targets.LambdaFunction(generator)]
        )


app = cdk.App()
MyStack(app, 'third-party-api-rate-limit')
app.synth()
