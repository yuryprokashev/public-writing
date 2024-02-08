#!/usr/bin/env python3
import aws_cdk as cdk
from constructs import Construct
from aws_cdk import aws_kinesisfirehose as firehose


class MyStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        bucket_name = self.node.get_context('bucketName')
        # Glue Database
        glue_database_name = 'million_records_on_s3'
        glue_database_input = cdk.aws_glue.CfnDatabase.DatabaseInputProperty(
            name=glue_database_name,
            location_uri=f's3://{bucket_name}/glue-db/'
        )
        glue_database = cdk.aws_glue.CfnDatabase(
            self, glue_database_name,
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=glue_database_input
        )
        common_columns = [
            ('timestamp', 'timestamp'),
            ('symbol_key', 'string'),
            ('parameter_key', 'string'),
            ('parameter_value', 'double')
        ]
        json_data_table = self.json_data_table(glue_database_name, columns=common_columns)
        json_data_table.add_dependency(glue_database)

        json_data_collection_table = self.json_data_collection_table(glue_database_name, columns=common_columns)
        json_data_collection_table.add_dependency(glue_database)

        parquet_data_table = self.parquet_data_table(glue_database_name, columns=common_columns)
        parquet_data_table.add_dependency(glue_database)

        kinesis_data_table = self.kinesis_data_table(glue_database_name, columns=common_columns)
        kinesis_data_table.add_dependency(glue_database)

        firehose_parquet = self.firehose_parquet(
            table_name='kinesis_parquet_data',
            database_name=glue_database_name,
            database_s3_prefix='glue-db',
            buffer_in_sec=60
        )
        self.producer(firehose_parquet)

    def json_data_table(self, database_name, columns):
        bucket_name = self.node.get_context('bucketName')
        _columns = [
            cdk.aws_glue.CfnTable.ColumnProperty(name=col_name, type=col_type)
            for col_name, col_type in columns
        ]
        table_input = cdk.aws_glue.CfnTable.TableInputProperty(
            name='json_data',
            parameters={
                'classification': 'json'
            },
            storage_descriptor=cdk.aws_glue.CfnTable.StorageDescriptorProperty(
                columns=_columns,
                location=f's3://{bucket_name}/glue-db/json-data/',
                input_format='org.apache.hadoop.mapred.TextInputFormat',
                output_format='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                serde_info=cdk.aws_glue.CfnTable.SerdeInfoProperty(
                    serialization_library='org.openx.data.jsonserde.JsonSerDe'
                )
            ),
            table_type='EXTERNAL_TABLE'
        )
        return cdk.aws_glue.CfnTable(
            self, 'json-data-table',
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_name=database_name,
            table_input=table_input,
        )

    def json_data_collection_table(self, database_name, columns):
        bucket_name = self.node.get_context('bucketName')
        _columns = [
            cdk.aws_glue.CfnTable.ColumnProperty(name=col_name, type=col_type)
            for col_name, col_type in columns
        ]
        table_input = cdk.aws_glue.CfnTable.TableInputProperty(
            name='json_data_collection',
            parameters={
                'classification': 'json'
            },
            storage_descriptor=cdk.aws_glue.CfnTable.StorageDescriptorProperty(
                columns=_columns,
                location=f's3://{bucket_name}/glue-db/json-data-collection/',
                input_format='org.apache.hadoop.mapred.TextInputFormat',
                output_format='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                serde_info=cdk.aws_glue.CfnTable.SerdeInfoProperty(
                    serialization_library='org.openx.data.jsonserde.JsonSerDe'
                )
            ),
            table_type='EXTERNAL_TABLE'
        )
        return cdk.aws_glue.CfnTable(
            self, 'json-data-collection-table',
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_name=database_name,
            table_input=table_input,
        )

    def parquet_data_table(self, database_name, columns):
        bucket_name = self.node.get_context('bucketName')
        _columns = [
            cdk.aws_glue.CfnTable.ColumnProperty(name=col_name, type=col_type)
            for col_name, col_type in columns
        ]
        table_input = cdk.aws_glue.CfnTable.TableInputProperty(
            name='parquet_data',
            storage_descriptor=cdk.aws_glue.CfnTable.StorageDescriptorProperty(
                columns=_columns,
                location=f's3://{bucket_name}/glue-db/parquet-data/',
                input_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                output_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                serde_info=cdk.aws_glue.CfnTable.SerdeInfoProperty(
                    serialization_library='org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                )
            )
        )
        return cdk.aws_glue.CfnTable(
            self, 'parquet-data-table',
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_name=database_name,
            table_input=table_input,
        )

    def kinesis_data_table(self, database_name, columns):
        bucket_name = self.node.get_context('bucketName')
        _columns = [
            cdk.aws_glue.CfnTable.ColumnProperty(name=col_name, type=col_type)
            for col_name, col_type in columns
        ]
        table_input = cdk.aws_glue.CfnTable.TableInputProperty(
            name='kinesis_parquet_data',
            storage_descriptor=cdk.aws_glue.CfnTable.StorageDescriptorProperty(
                columns=_columns,
                location=f's3://{bucket_name}/glue-db/kinesis-parquet-data/',
                input_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                output_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                serde_info=cdk.aws_glue.CfnTable.SerdeInfoProperty(
                    serialization_library='org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                )
            )
        )
        return cdk.aws_glue.CfnTable(
            self, 'kinesis-parquet-data-table',
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_name=database_name,
            table_input=table_input,
        )

    def producer(self, firehose_parquet):
        bucket_name = self.node.get_context('bucketName')
        producer = cdk.aws_lambda.Function(
            self, 'producer',
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_11,
            handler='producer.handler',
            code=cdk.aws_lambda.Code.from_asset('./src'),
            timeout=cdk.Duration.seconds(900),
            memory_size=4096,
            architecture=cdk.aws_lambda.Architecture.ARM_64,
            reserved_concurrent_executions=1,
            environment={
                'BUCKET_NAME': bucket_name,
                'FIREHOSE_NAME': firehose_parquet.ref
            }
        )
        producer.add_to_role_policy(
            cdk.aws_iam.PolicyStatement(
                actions=[
                    's3:PutObject',
                    's3:GetObject'
                ],
                resources=[
                    f'arn:aws:s3:::{bucket_name}/*',
                    f'arn:aws:s3:::{bucket_name}',
                ]
            )
        )
        producer.add_to_role_policy(
            cdk.aws_iam.PolicyStatement(
                actions=['firehose:PutRecordBatch'],
                resources=[firehose_parquet.attr_arn]
            )
        )

    def firehose_parquet(self, table_name, database_name, database_s3_prefix, buffer_in_sec) -> firehose.CfnDeliveryStream:
        bucket_name = self.node.get_context('bucketName')
        if '-' in table_name:
            raise Exception('Table name can\'t contain hyphens')
        database_bucket_arn = f'arn:aws:s3:::{bucket_name}'
        db_s3_location = f'{database_bucket_arn}/{database_s3_prefix}'
        table_name_hyphened = table_name.replace("_", "-")
        s3_data_location = f'{database_s3_prefix}/{table_name_hyphened}/'
        s3_error_location = f'{database_s3_prefix}/error/{table_name_hyphened}/'
        print('s3_data_location', s3_data_location)

        firehose_name = f'{table_name_hyphened}-writer'

        firehose_role = cdk.aws_iam.Role(
            self, f'{firehose_name}-role',
            assumed_by=cdk.aws_iam.ServicePrincipal("firehose.amazonaws.com"),
            managed_policies=[
                cdk.aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                cdk.aws_iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess")
            ]
        )
        firehose_role.add_to_policy(cdk.aws_iam.PolicyStatement(
            actions=["glue:getTableVersions", "glue:GetSchema", "glue:GetSchemaVersion"],
            resources=["*"]
        ))
        firehose_role.add_to_policy(cdk.aws_iam.PolicyStatement(
            actions=['s3:PutObject'],
            resources=[f'{db_s3_location}/*']
        ))
        firehose_buffering_hints = firehose.CfnDeliveryStream.BufferingHintsProperty(
            interval_in_seconds=buffer_in_sec,
            size_in_m_bs=128
        )
        processors = [
            firehose.CfnDeliveryStream.ProcessorProperty(
                type='AppendDelimiterToRecord',
                parameters=[
                    firehose.CfnDeliveryStream.ProcessorParameterProperty(
                        parameter_name='Delimiter',
                        parameter_value='\\n'
                    )
                ]
            )
        ]
        firehose_processing_config = firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
            enabled=True,
            processors=processors
        )
        firehose_input_format_config = firehose.CfnDeliveryStream.InputFormatConfigurationProperty(
            deserializer=firehose.CfnDeliveryStream.DeserializerProperty(
                open_x_json_ser_de=firehose.CfnDeliveryStream.OpenXJsonSerDeProperty(
                    case_insensitive=False,
                    convert_dots_in_json_keys_to_underscores=True
                )
            )
        )
        firehose_output_format_config = firehose.CfnDeliveryStream.OutputFormatConfigurationProperty(
            serializer=firehose.CfnDeliveryStream.SerializerProperty(
                parquet_ser_de=firehose.CfnDeliveryStream.ParquetSerDeProperty(
                    compression='SNAPPY',
                )
            )
        )
        firehose_schema_config = firehose.CfnDeliveryStream.SchemaConfigurationProperty(
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_name=database_name,
            table_name=table_name,
            region=cdk.Aws.REGION,
            role_arn=firehose_role.role_arn
        )
        firehose_data_format_conversion_config = firehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
            enabled=True,
            input_format_configuration=firehose_input_format_config,
            output_format_configuration=firehose_output_format_config,
            schema_configuration=firehose_schema_config
        )
        firehose_destination = firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
            bucket_arn=database_bucket_arn,
            role_arn=firehose_role.role_arn,
            buffering_hints=firehose_buffering_hints,
            prefix=s3_data_location,
            error_output_prefix=s3_error_location,
            processing_configuration=firehose_processing_config,
            data_format_conversion_configuration=firehose_data_format_conversion_config,
            cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                enabled=False
            )
        )
        return firehose.CfnDeliveryStream(
            self, firehose_name,
            delivery_stream_type='DirectPut',
            extended_s3_destination_configuration=firehose_destination
        )


app = cdk.App()
MyStack(app, 'million-records-on-s3')
app.synth()
