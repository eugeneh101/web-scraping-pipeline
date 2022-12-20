from aws_cdk import (
    BundlingOptions,
    Duration,
    RemovalPolicy,
    Stack,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as _lambda_event_sources,
    aws_redshift as redshift,
    aws_s3 as s3,
    aws_sqs as sqs,
)
from constructs import Construct


class RedshiftService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        # security_group: ec2.SecurityGroup,
    ) -> None:
        super().__init__(scope, construct_id)  # required
        self.redshift_full_commands_full_access_role = iam.Role(
            self,
            "RedshiftClusterRole",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonRedshiftAllCommandsFullAccess"
                ),  ### later principle of least privileges
            ],
        )
        self.redshift_cluster = redshift.CfnCluster(
            self,
            "RedshiftCluster",
            cluster_type="single-node",  # for demo purposes
            number_of_nodes=1,  # for demo purposes
            node_type="dc2.large",  # for demo purposes
            db_name=environment["REDSHIFT_DATABASE_NAME"],
            master_username=environment["REDSHIFT_USER"],
            master_user_password=environment["REDSHIFT_PASSWORD"],
            iam_roles=[self.redshift_full_commands_full_access_role.role_arn],
            # cluster_subnet_group_name=demo_cluster_subnet_group.ref,
            # vpc_security_group_ids=[security_group.security_group_id],
        )


class WebScrapingPipelineStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Publish Message Stack
        # stateful resources
        self.scheduled_eventbridge_event = events.Rule(
            self,
            "RunEveryMinute",
            event_bus=None,  # scheduled events must be on "default" bus
            schedule=events.Schedule.rate(Duration.minutes(1)),
        )
        self.scraped_messages_queue = sqs.Queue(
            self,
            "ScrapedMessagesQueue",
            removal_policy=RemovalPolicy.DESTROY,
            retention_period=Duration.days(4),
            visibility_timeout=Duration.seconds(10),  # retry failed message quickly
        )

        # stateless resources
        self.publish_messages_to_sqs_lambda = _lambda.Function(
            self,
            "PublishMessagesToSQSLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/publish_messages_to_sqs_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py data-engineering-bezant-assignement-dataset.zip /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(1),  # should be fairly quick
            memory_size=1024,  # in MB
        )

        # connect AWS resources
        self.scheduled_eventbridge_event.add_target(
            target=events_targets.LambdaFunction(
                handler=self.publish_messages_to_sqs_lambda,
                retry_attempts=3,
                ### then put in DLQ
            ),
        )
        self.scraped_messages_queue.grant_send_messages(self.publish_messages_to_sqs_lambda)
        self.publish_messages_to_sqs_lambda.add_environment(
            key="QUEUE_NAME", value=self.scraped_messages_queue.queue_name
        )


        # Store Message Stack
        self.redshift_service = RedshiftService(
            self, "RedshiftService", environment=environment
        # security_group: ec2.SecurityGroup,
        )
        self.s3_bucket_for_redshift_staging = s3.Bucket(
            self,
            "S3BucketForRedshiftStaging",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=False,  # if versioning disabled, then expired files are deleted
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="expire_files_with_certain_prefix_after_1_day",
                    expiration=Duration.days(1),
                    prefix=f"{environment['PROCESSED_SQS_MESSAGES_FOLDER']}/",
                ),
            ],
        )

        self.lambda_redshift_full_access_role = iam.Role(
            self,
            "LambdaRedshiftFullAccessRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonRedshiftFullAccess"
                ),  ### later principle of least privileges
            ],
        )
        self.write_messages_to_redshift_lambda = _lambda.Function(
            self,
            "WriteMessagesToRedshiftLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/write_messages_to_redshift_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(5),  # should be fairly quick
            memory_size=1024,  # in MB
            environment={
                "REDSHIFT_USER": environment["REDSHIFT_USER"],
                "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
                "REDSHIFT_SCHEMA_NAME": environment["REDSHIFT_SCHEMA_NAME"],
                "REDSHIFT_TABLE_NAME": environment["REDSHIFT_TABLE_NAME"],
                "UNPROCESSED_SQS_MESSAGES_FOLDER": environment["UNPROCESSED_SQS_MESSAGES_FOLDER"],
                "PROCESSED_SQS_MESSAGES_FOLDER": environment["PROCESSED_SQS_MESSAGES_FOLDER"],
                "AWSREGION": environment["AWS_REGION"],  # apparently "AWS_REGION" is not allowed as a Lambda env variable
            },
            role=self.lambda_redshift_full_access_role,
        )


        # # connect AWS resources
        self.write_messages_to_redshift_lambda.add_event_source(
            _lambda_event_sources.SqsEventSource(self.scraped_messages_queue, batch_size=1)
        )
        lambda_environment_variables = {
            "REDSHIFT_ENDPOINT_ADDRESS": self.redshift_service.redshift_cluster.attr_endpoint_address,
            "REDSHIFT_ROLE_ARN": self.redshift_service.redshift_full_commands_full_access_role.role_arn,
            "S3_BUCKET_FOR_REDSHIFT_STAGING": self.s3_bucket_for_redshift_staging.bucket_name,
            }
        for key, value in lambda_environment_variables.items():
            self.write_messages_to_redshift_lambda.add_environment(
                key=key, value=value
            )
