#!/usr/bin/env python3
import aws_cdk as cdk
from constructs import Construct


class MyStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.talib_python_layer(python_version='3.9', architecture='arm64')

    def talib_python_layer(self, python_version, architecture):
        image = cdk.DockerImage.from_registry(f'public.ecr.aws/lambda/python:{python_version}-{architecture}')
        lambda_layer_bundling_options = cdk.BundlingOptions(
            image=image,
            command=[
                f'''
                bash build-layer.sh {python_version}
                '''
            ],
            user='root',
            entrypoint=["/bin/sh", "-c"]  # because we need to override the default entry point of lambda runtime
        )
        path = './layer'

        layer_name = f'talib-{python_version}-{architecture}-layer'.replace('.', '-')
        lambda_layer = cdk.aws_lambda.LayerVersion(
            self, f'talib-{python_version}-{architecture}-layer',
            layer_version_name=layer_name,
            code=cdk.aws_lambda.Code.from_asset(
                path=path,
                bundling=lambda_layer_bundling_options
            ),
            compatible_runtimes=[self.python_runtime_from_version(python_version)],
            compatible_architectures=[self.architecture_from_string(architecture)],
            description=f'TA-lib layer for python {python_version} on {architecture}',
            removal_policy=cdk.RemovalPolicy.RETAIN
        )
        cdk.CfnOutput(
            self, 'LambdaLayerArn',
            value=lambda_layer.layer_version_arn
        )

    def python_runtime_from_version(self, python_version):
        if python_version == '3.9':
            return cdk.aws_lambda.Runtime.PYTHON_3_9
        elif python_version == '3.11':
            return cdk.aws_lambda.Runtime.PYTHON_3_11
        elif python_version == '3.12':
            return cdk.aws_lambda.Runtime.PYTHON_3_12
        else:
            raise ValueError(f'Unsupported python version: {python_version}')

    def architecture_from_string(self, architecture):
        if architecture == 'arm64':
            return cdk.aws_lambda.Architecture.ARM_64
        elif architecture == 'x86_64':
            return cdk.aws_lambda.Architecture.X86_64
        else:
            raise ValueError(f'Unsupported architecture: {architecture}')


app = cdk.App()
MyStack(app, 'talib-layer')
app.synth()
