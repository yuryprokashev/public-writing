#!/usr/bin/env python3
import aws_cdk as cdk
from constructs import Construct


class AthenaGroupDemo(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        athena_bucket_name = self.node.get_context('athena_bucket_name')
        athena_bucket = cdk.aws_s3.Bucket.from_bucket_name(
            self, 'athena-bucket',
            bucket_name=athena_bucket_name
        )

        # Create Athena WorkGroup and add tags to it
        athena_workgroup_configuration = cdk.aws_athena.CfnWorkGroup.WorkGroupConfigurationProperty(
            result_configuration=cdk.aws_athena.CfnWorkGroup.ResultConfigurationProperty(
                output_location=f's3://{athena_bucket.bucket_name}/athena',
            ),
            publish_cloud_watch_metrics_enabled=True,
            bytes_scanned_cutoff_per_query=20 * 1024 * 1024 * 1024  # 20Gb
        )

        athena_workgroup = cdk.aws_athena.CfnWorkGroup(
            self, 'my-app-athena-workgroup',
            name='dev.my-company.my-app',
            state='ENABLED',
            work_group_configuration=athena_workgroup_configuration,
            tags=[
                cdk.CfnTag(key='Environment', value='dev'),
                cdk.CfnTag(key='Company', value='my-company'),
                cdk.CfnTag(key='Product', value='my-app')
            ]
        )
        athena_workgroup.apply_removal_policy(cdk.RemovalPolicy.RETAIN)

        # Monitor Average Bytes Scanned Per Query
        average_processed_bytes = cdk.aws_cloudwatch.Metric(
            namespace='AWS/Athena',
            metric_name='ProcessedBytes',
            dimensions_map={
                'WorkGroup': athena_workgroup.name
            },
            period=cdk.Duration.minutes(1),
            statistic='Average'
        )

        average_processed_bytes_alarm = cdk.aws_cloudwatch.Alarm(
            self, 'my-app-component-athena-average-processed-bytes-alarm',
            metric=average_processed_bytes,
            threshold=10 * 1024 * 1024 * 1024,
            evaluation_periods=1,
            comparison_operator=cdk.aws_cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            alarm_name='my-app-component-athena-average-processed-bytes-alarm',
            alarm_description='Alarm if average processed bytes is greater than 10Gb',
            actions_enabled=True
        )

        # Monitor Total Queries Per Day
        number_of_queries_1day_sum_metric = cdk.aws_cloudwatch.Metric(
            namespace='AWS/Athena',
            metric_name='ProcessedBytes',
            dimensions_map={
                'WorkGroup': athena_workgroup.name
            },
            period=cdk.Duration.hours(24),
            statistic='SampleCount'
        )
        total_queries_1day_alarm = cdk.aws_cloudwatch.Alarm(
            self, 'my-app-component-athena-total-queries-1day-alarm',
            metric=number_of_queries_1day_sum_metric,
            threshold=1000,
            evaluation_periods=1,
            comparison_operator=cdk.aws_cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            alarm_name='my-app-component-athena-total-queries-1day-alarm',
            alarm_description='Alarm if total queries per day is greater than 1000',
            actions_enabled=True
        )

        # Send Email on Both Alarms
        processed_bytes_sns_topic = cdk.aws_sns.Topic(
            self, 'my-app-component-athena-processed-bytes-sns-topic',
            display_name='my-app-component-athena-processed-bytes-sns-topic'
        )
        email_address = self.node.get_context('admin_email')
        topic_email_subscription = cdk.aws_sns_subscriptions.EmailSubscription(
            email_address=email_address
        )
        processed_bytes_sns_topic.add_subscription(topic_email_subscription)

        average_processed_bytes_alarm.add_alarm_action(
            cdk.aws_cloudwatch_actions.SnsAction(processed_bytes_sns_topic)
        )
        total_queries_1day_alarm.add_alarm_action(
            cdk.aws_cloudwatch_actions.SnsAction(processed_bytes_sns_topic)
        )


app = cdk.App()
AthenaGroupDemo(app, 'athena-workgroup-demo')
app.synth()
