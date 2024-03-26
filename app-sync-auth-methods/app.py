#!/usr/bin/env python3
from urllib.parse import urlparse

import aws_cdk as cdk
from constructs import Construct


class MyStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        user_pool, user_pool_client = self.user_pool()
        cdk.CfnOutput(
            self, 'UserPoolId', value=user_pool.user_pool_id
        )
        cdk.CfnOutput(
            self, 'UserPoolClientId', value=user_pool_client.user_pool_client_id
        )
        api = self.process_api(user_pool)
        web_site_server_url, web_site_server = self.web_site_server(api)

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
            architecture=cdk.aws_lambda.Architecture.ARM_64
        )

        function_url = web_site_server.add_function_url(
            auth_type=cdk.aws_lambda.FunctionUrlAuthType.NONE,
            cors=cdk.aws_lambda.FunctionUrlCorsOptions(allowed_origins=['*'])
        )
        return function_url, web_site_server

    def process_api(self, user_pool) -> cdk.aws_appsync.GraphqlApi:
        authorizer = cdk.aws_lambda.Function(
            self, 'authorizer',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='authorizer.main',
            code=cdk.aws_lambda.Code.from_asset(
                path='./src',
                exclude=['*', '!authorizer.py']
            ),
            architecture=cdk.aws_lambda.Architecture.ARM_64
        )
        lambda_auth_mode = cdk.aws_appsync.AuthorizationMode(
            authorization_type=cdk.aws_appsync.AuthorizationType.LAMBDA,
            lambda_authorizer_config=cdk.aws_appsync.LambdaAuthorizerConfig(
                handler=authorizer
            )
        )
        user_pool_auth_mode = cdk.aws_appsync.AuthorizationMode(
            authorization_type=cdk.aws_appsync.AuthorizationType.USER_POOL,
            user_pool_config=cdk.aws_appsync.UserPoolConfig(
                user_pool=user_pool
            )
        )
        graphql_api = cdk.aws_appsync.GraphqlApi(
            self, 'process-api',
            name='experiment-appsync-api-with-lambda-authorizer',
            definition=cdk.aws_appsync.Definition.from_file(
                file_path='./schema.graphql'
            ),
            authorization_config=cdk.aws_appsync.AuthorizationConfig(
                default_authorization=user_pool_auth_mode,
                additional_authorization_modes=[lambda_auth_mode]
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
        start_process_lambda_lambda = cdk.aws_lambda.Function(
            self, 'start-process-lambda-lambda',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='api.start_process_lambda',
            code=cdk.aws_lambda.Code.from_asset(
                path='./src',
                exclude=['*', '!api.py']
            ),
            architecture=cdk.aws_lambda.Architecture.ARM_64
        )
        start_process_lambda_datasource = graphql_api.add_lambda_data_source(
            'start-process-lambda-datasource',
            lambda_function=start_process_lambda_lambda
        )
        start_process_lambda_datasource.create_resolver(
            id='start-process-lambda-resolver',
            type_name='Mutation',
            field_name='startProcessLambda'
        )
        return graphql_api

    def user_pool(self) -> tuple[cdk.aws_cognito.UserPool, cdk.aws_cognito.UserPoolClient]:

        user_pool = cdk.aws_cognito.UserPool(
            self, 'user-pool',
            user_pool_name='experiment-app-sync-user-pool',
            self_sign_up_enabled=True,
            sign_in_aliases=cdk.aws_cognito.SignInAliases(
                email=True
            ),
            standard_attributes=cdk.aws_cognito.StandardAttributes(
                email=cdk.aws_cognito.StandardAttribute(required=True, mutable=True)
            ),
            custom_attributes={
                'usage_plan': cdk.aws_cognito.StringAttribute(mutable=True)
            },
            password_policy=cdk.aws_cognito.PasswordPolicy(
                min_length=8,
                require_digits=True,
                require_lowercase=True,
                require_uppercase=True,
                require_symbols=True
            ),
            account_recovery=cdk.aws_cognito.AccountRecovery.EMAIL_ONLY,
            auto_verify=cdk.aws_cognito.AutoVerifiedAttrs(
                email=True
            ),
            sign_in_case_sensitive=True
        )
        user_pool_client = user_pool.add_client(
            'web-site-client',
            auth_flows=cdk.aws_cognito.AuthFlow(
                user_password=True,
                user_srp=True
            ),
            refresh_token_validity=cdk.Duration.days(30),
        )
        return user_pool, user_pool_client


app = cdk.App()
MyStack(app, 'experiment-lambda-authorizer-with-appsync')
app.synth()
