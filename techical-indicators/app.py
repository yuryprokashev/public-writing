#!/usr/bin/env python3

import aws_cdk as cdk
from constructs import Construct


class MyStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        talib_layer_arn = self.node.try_get_context('talibLayerArn')
        if talib_layer_arn:
            talib_layer = cdk.aws_lambda.LayerVersion.from_layer_version_arn(
                self, 'talib-layer',
                layer_version_arn=talib_layer_arn
            )
        else:
            talib_layer = self.talib_python_layer(
                name='talib-python3_11-arm64',
            )
        cdk.CfnOutput(
            self, 'TALibLayerArn',
            value=talib_layer.layer_version_arn
        )
        lambda_function_url = self.render_mfi_chart(layers=[talib_layer])
        cdk.CfnOutput(
            self, 'RenderMFIChartFunctionUrl',
            value=lambda_function_url.url
        )

    def talib_python_layer(self, name):
        image = cdk.DockerImage.from_registry(f'public.ecr.aws/lambda/python:3.11-arm64')
        lambda_layer_bundling_options = cdk.BundlingOptions(
            image=image,
            command=[
                f'''
                bash build-layer.sh 3.11
                '''
            ],
            user='root',
            entrypoint=["/bin/sh", "-c"]  # because we need to override the default entry point of lambda runtime
        )
        path = './talib-layer'
        return cdk.aws_lambda.LayerVersion(
            self, f'talib-3_11-arm64-layer',
            layer_version_name=name,
            code=cdk.aws_lambda.Code.from_asset(
                path=path,
                bundling=lambda_layer_bundling_options
            ),
            compatible_runtimes=[cdk.aws_lambda.Runtime.PYTHON_3_11],
            compatible_architectures=[cdk.aws_lambda.Architecture.ARM_64],
            description=f'TA-lib layer for python 3.11 on ARM64 architecture',
            removal_policy=cdk.RemovalPolicy.RETAIN
        )

    def render_mfi_chart(self, layers):
        lambda_function = cdk.aws_lambda.Function(
            self, 'RenderMFIChartFunction',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            architecture=cdk.aws_lambda.Architecture.ARM_64,
            handler='render_mfi_chart.main',
            code=cdk.aws_lambda.Code.from_asset('src'),
            layers=layers,
            memory_size=512,
            timeout=cdk.Duration.seconds(30)
        )
        url = lambda_function.add_function_url(
            auth_type=cdk.aws_lambda.FunctionUrlAuthType.NONE,
            cors=cdk.aws_lambda.FunctionUrlCorsOptions(allowed_origins=['*'])
        )
        return url


app = cdk.App()
MyStack(app, 'technical-indicators')
app.synth()
