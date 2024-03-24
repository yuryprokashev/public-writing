#!/usr/bin/env python3
from urllib.parse import urlparse

import aws_cdk as cdk
from constructs import Construct


class MyStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        api = self.process_api()
        web_site_server_url, web_site_server = self.web_site_server(api)
        async_process = self.async_process()
        async_process_start_rule = self.async_process_start_rule()
        async_process_done_rule = self.async_process_done_rule()
        async_process_listener = self.async_process_listener(graphql_api=api)

        async_process_start_rule.add_target(cdk.aws_events_targets.LambdaFunction(async_process))
        async_process_done_rule.add_target(cdk.aws_events_targets.LambdaFunction(async_process_listener))

        cdk.CfnOutput(self, 'WebSiteUrl', value=web_site_server_url.url)

    def web_site_server(self, graphql_api) -> tuple[cdk.aws_lambda.FunctionUrl, cdk.aws_lambda.Function]:
        web_site_server = cdk.aws_lambda.Function(
            self, 'web-site-server',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='web_site_server.main',
            code=cdk.aws_lambda.Code.from_asset(
                path='./src',
                exclude=['*', '!web_site_server.py', '!index.html', '!frontend', '!frontend/dist', '!frontend/dist/*']
            ),
            architecture=cdk.aws_lambda.Architecture.ARM_64,
            environment={
                'API_URL': graphql_api.graphql_url,
                'API_KEY': graphql_api.api_key
            }
        )
        function_url = web_site_server.add_function_url(
            auth_type=cdk.aws_lambda.FunctionUrlAuthType.NONE,
            cors=cdk.aws_lambda.FunctionUrlCorsOptions(allowed_origins=['*'])
        )
        return function_url, web_site_server

    def process_api(self) -> cdk.aws_appsync.GraphqlApi:
        graphql_api = cdk.aws_appsync.GraphqlApi(
            self, 'process-api',
            name='tutorial-async-process-api',
            definition=cdk.aws_appsync.Definition.from_file(
                file_path='./schema.graphql'
            )
        )

        start_process_lambda = cdk.aws_lambda.Function(
            self, 'start-process-lambda',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='api.start_process',
            code=cdk.aws_lambda.Code.from_asset(
                path='./src',
                exclude=['*', '!api.py']
            ),
            architecture=cdk.aws_lambda.Architecture.ARM_64,
            environment={
                'SOURCE': 'tutorial.async-process-api'
            }
        )
        start_process_datasource = graphql_api.add_lambda_data_source(
            'start-process-datasource',
            lambda_function=start_process_lambda
        )
        start_process_datasource.create_resolver(
            id='start-process-resolver',
            type_name='Mutation',
            field_name='startProcess'
        )
        start_process_lambda.add_to_role_policy(
            statement=cdk.aws_iam.PolicyStatement(
                actions=['events:PutEvents'],
                resources=['*']
            )
        )

        end_process_lambda = cdk.aws_lambda.Function(
            self, 'end-process-lambda',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='api.end_process',
            code=cdk.aws_lambda.Code.from_asset(
                path='./src',
                exclude=['*', '!api.py']
            ),
            architecture=cdk.aws_lambda.Architecture.ARM_64
        )
        end_process_datasource = graphql_api.add_lambda_data_source(
            'end-process-datasource',
            lambda_function=end_process_lambda
        )
        end_process_datasource.create_resolver(
            id='end-process-resolver',
            type_name='Mutation',
            field_name='endProcess'
        )
        return graphql_api

    def async_process(self) -> cdk.aws_lambda.Function:
        lambda_function = cdk.aws_lambda.Function(
            self, 'async-process',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='async_process.main',
            code=cdk.aws_lambda.Code.from_asset(
                path='./src',
                exclude=['*', '!async_process.py']
            ),
            architecture=cdk.aws_lambda.Architecture.ARM_64,
            timeout=cdk.Duration.minutes(15),
            environment={
                'SOURCE': 'tutorial.async-process'
            }
        )
        lambda_function.add_to_role_policy(
            statement=cdk.aws_iam.PolicyStatement(
                actions=['events:PutEvents'],
                resources=['*']
            )
        )
        return lambda_function

    def async_process_done_rule(self) -> cdk.aws_events.Rule:
        return cdk.aws_events.Rule(
            self, 'async-process-rule',
            event_pattern=cdk.aws_events.EventPattern(
                source=['tutorial.async-process'],
                detail_type=['async-process-event'],
                detail={
                    'status': ['done']
                }
            )
        )

    def async_process_start_rule(self) -> cdk.aws_events.Rule:
        return cdk.aws_events.Rule(
            self, 'async-process-start-rule',
            event_pattern=cdk.aws_events.EventPattern(
                source=['tutorial.async-process-api'],
                detail_type=['async-process-event'],
                detail={
                    'status': ['start']
                }
            )
        )

    def async_process_listener(self, graphql_api) -> cdk.aws_lambda.Function:
        lambda_function = cdk.aws_lambda.Function(
            self, 'async-process-listener',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='api.async_process_listener',
            code=cdk.aws_lambda.Code.from_asset(
                path='./src',
                exclude=['*', '!api.py']
            ),
            architecture=cdk.aws_lambda.Architecture.ARM_64,
            environment={
                'API_URL': graphql_api.graphql_url,
                'API_KEY': graphql_api.api_key
            }
        )
        lambda_function.add_to_role_policy(
            statement=cdk.aws_iam.PolicyStatement(
                actions=['appsync:GraphQL'],
                resources=[graphql_api.arn]
            )
        )
        return lambda_function


app = cdk.App()
MyStack(app, 'tutorial-async-process-done')
app.synth()
