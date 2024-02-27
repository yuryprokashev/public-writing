import aws_cdk as cdk
from constructs import Construct


def parquet_table_storage(
        columns, s3_location
) -> cdk.aws_glue.CfnTable.StorageDescriptorProperty:
    columns = [
        cdk.aws_glue.CfnTable.ColumnProperty(name=col_name, type=col_type)
        for col_name, col_type in columns
    ]
    return cdk.aws_glue.CfnTable.StorageDescriptorProperty(
        columns=columns,
        location=s3_location,
        input_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        output_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        serde_info=cdk.aws_glue.CfnTable.SerdeInfoProperty(
            serialization_library='org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        )
    )


def csv_table_storage(columns, s3_location) -> cdk.aws_glue.CfnTable.StorageDescriptorProperty:
    columns = [
        cdk.aws_glue.CfnTable.ColumnProperty(name=col_name, type=col_type)
        for col_name, col_type in columns
    ]
    return cdk.aws_glue.CfnTable.StorageDescriptorProperty(
        columns=columns,
        location=s3_location,
        input_format='org.apache.hadoop.mapred.TextInputFormat',
        output_format='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        serde_info=cdk.aws_glue.CfnTable.SerdeInfoProperty(
            serialization_library='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
            parameters={'field.delim': ',', 'escape.delim': '\\'}
        )
    )


def nd_json_table_storage(columns, s3_location) -> cdk.aws_glue.CfnTable.StorageDescriptorProperty:
    columns = [
        cdk.aws_glue.CfnTable.ColumnProperty(name=col_name, type=col_type)
        for col_name, col_type in columns
    ]
    return cdk.aws_glue.CfnTable.StorageDescriptorProperty(
        columns=columns,
        location=s3_location,
        input_format='org.apache.hadoop.mapred.TextInputFormat',
        output_format='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        serde_info=cdk.aws_glue.CfnTable.SerdeInfoProperty(
            serialization_library='org.openx.data.jsonserde.JsonSerDe'
        )
    )


class StockAnalyzerStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        bucket, glue_database = self.data_layer()
        lambda_layer = self.lambda_layer()
        self.candle_loader(
            bucket=bucket,
            layer=lambda_layer
        )
        self.daily_returns_by_date(
            bucket=bucket,
            glue_database=glue_database,
            layer=lambda_layer
        )

    def data_layer(self) -> tuple[cdk.aws_s3.Bucket, cdk.aws_glue.CfnDatabase]:
        # Step 1: Create an S3 bucket
        bucket = cdk.aws_s3.Bucket(
            self, 'stock-analyzer-bucket'
        )
        # Step 2: Create a Glue database
        glue_database_name = 'stock_analyzer'
        glue_database_input = cdk.aws_glue.CfnDatabase.DatabaseInputProperty(
            name=glue_database_name,
            location_uri=f's3://{bucket.bucket_name}/glue-db/'
        )
        glue_database = cdk.aws_glue.CfnDatabase(
            self, 'stock-analyzer-database',
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=glue_database_input
        )
        # Step 3: Create a Glue table
        parquet_storage = parquet_table_storage(
            columns=[
                ('timestamp', 'timestamp'), ('symbol', 'string'),
                ('open', 'double'), ('high', 'double'),
                ('low', 'double'), ('close', 'double'),
                ('volume', 'bigint')
            ],
            s3_location=f's3://{bucket.bucket_name}/glue-db/candle/'
        )
        candle_table = self.glue_table(
            database_name=glue_database.database_input.name,
            table_name='candle',
            storage_descriptor=parquet_storage
        )
        candle_table.add_dependency(glue_database)
        return bucket, glue_database

    def glue_table(
            self,
            database_name,
            table_name,
            storage_descriptor,
            catalog_id=cdk.Aws.ACCOUNT_ID
    ) -> cdk.aws_glue.CfnTable:
        if '-' in table_name:
            raise Exception('Table name can\'t contain hyphens')

        table_input = cdk.aws_glue.CfnTable.TableInputProperty(
            name=table_name,
            storage_descriptor=storage_descriptor,
        )
        table_id = f'table-{table_name.replace("_", "-")}'
        return cdk.aws_glue.CfnTable(
            self,
            table_id,
            catalog_id=catalog_id,
            database_name=database_name,
            table_input=table_input
        )

    def lambda_layer(self) -> cdk.aws_lambda.LayerVersion:
        layer_base_image = cdk.DockerImage.from_registry('public.ecr.aws/lambda/python:3.12-x86_64')
        build_layer_command = f'''
                mkdir -p /asset-output/python/lib/python3.12/site-packages
                pip install -r /asset-input/requirements.txt -t /asset-output/python/lib/python3.12/site-packages
                '''
        layer_code = cdk.aws_lambda.Code.from_asset(
            path='layer',
            bundling=cdk.BundlingOptions(
                image=layer_base_image,
                command=[build_layer_command],
                user='root',
                entrypoint=["/bin/sh", "-c"]
            )
        )
        return cdk.aws_lambda.LayerVersion(
            self, 'stock-analyzer-lambda-layer',
            code=layer_code,
            compatible_runtimes=[cdk.aws_lambda.Runtime.PYTHON_3_12],
            description='yfinance, fastparquet, numpy, urllib3, jsonschema for Python 3.12',
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

    def candle_loader(self, bucket, layer) -> None:
        lambda_function = cdk.aws_lambda.Function(
            self, 'candle-loader',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_12,
            code=cdk.aws_lambda.Code.from_asset(
                path='stock_analyzer',
                exclude=['*', '!candle_loader.py'],
            ),
            handler='candle_loader.main',
            environment={
                'BUCKET_NAME': bucket.bucket_name,
                'CANDLE_S3_PREFIX': 'glue-db/candle/'
            },
            layers=[layer],
            timeout=cdk.Duration.minutes(15),
            memory_size=2048
        )
        lambda_function.add_to_role_policy(
            statement=cdk.aws_iam.PolicyStatement(
                actions=[
                    's3:PutObject'
                ],
                resources=[f'{bucket.bucket_arn}/glue-db/candle/*']
            )
        )
        daily_schedule_rule = cdk.aws_events.Rule(
            self, 'candle-loader-schedule',
            enabled=False,
            schedule=cdk.aws_events.Schedule.cron(
                minute='0',
                hour='2'
            )
        )
        daily_schedule_rule.add_target(
            cdk.aws_events_targets.LambdaFunction(lambda_function)
        )

    def daily_returns_by_date(self, bucket, glue_database, layer) -> None:
        # Athena Workgroup to control the costs of Athena queries
        # made by "Daily Returns By Date" function
        athena_workgroup = cdk.aws_athena.CfnWorkGroup(
            self, 'daily-returns-by-date-athena-workgroup',
            name='com.my-company.stock-analyzer.daily-returns-by-date',
            state='ENABLED',
            work_group_configuration=cdk.aws_athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=cdk.aws_athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f's3://{bucket.bucket_name}/athena/'
                )
            ),
            tags=[
                cdk.CfnTag(key='Environment', value='dev'),
                cdk.CfnTag(key='Company', value='my-company'),
                cdk.CfnTag(key='Product', value='stock-analyzer'),
                cdk.CfnTag(key='Component', value='daily-returns-by-date'),
            ]
        )
        athena_workgroup.apply_removal_policy(cdk.RemovalPolicy.RETAIN)

        # The lambda function that computes the Daily Returns By Date
        # and its permissions
        lambda_function = cdk.aws_lambda.Function(
            self, 'daily-returns-by-date',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_12,
            code=cdk.aws_lambda.Code.from_asset(
                path='stock_analyzer',
                exclude=['*', '!daily_returns_by_date.py'],
            ),
            handler='daily_returns_by_date.main',
            environment={
                'DATABASE_NAME': glue_database.database_input.name,
                'WORKGROUP_NAME': athena_workgroup.name,
            },
            timeout=cdk.Duration.seconds(30),
            memory_size=512,
            layers=[layer]
        )
        lambda_function.add_to_role_policy(cdk.aws_iam.PolicyStatement(
            actions=[
                'athena:StartQueryExecution',
                'athena:GetQueryExecution',
                'athena:GetQueryResults',
                'athena:GetWorkGroup',
            ],
            resources=[f'arn:aws:athena:{self.region}:{self.account}:workgroup/{athena_workgroup.name}'],
        ))
        lambda_function.add_to_role_policy(cdk.aws_iam.PolicyStatement(
            actions=['glue:GetTable', 'glue:GetDatabase', 'glue:GetTableVersions', 'glue:GetTableVersion'],
            resources=[
                f'arn:aws:glue:{self.region}:{self.account}:catalog',
                f'arn:aws:glue:{self.region}:{self.account}:database/{glue_database.database_input.name}',
                f'arn:aws:glue:{self.region}:{self.account}:table/{glue_database.database_input.name}/candle'
            ]
        ))
        lambda_function.add_to_role_policy(cdk.aws_iam.PolicyStatement(
            actions=[
                's3:GetObject',
                's3:GetObjectVersion',
                's3:ListBucket',
                's3:GetBucketLocation',
                's3:PutObject',
            ],
            resources=[
                f'arn:aws:s3:::{bucket.bucket_name}',
                f'arn:aws:s3:::{bucket.bucket_name}/*'
            ]
        ))

        # Expose the lambda function via the Function URL
        function_url = lambda_function.add_function_url(
            auth_type=cdk.aws_lambda.FunctionUrlAuthType.NONE,
            cors=cdk.aws_lambda.FunctionUrlCorsOptions(allowed_origins=['*'])
        )
        cdk.CfnOutput(
            self, 'DailyReturnsByDateUrl',
            value=function_url.url
        )
