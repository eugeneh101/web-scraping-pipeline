from aws_cdk import (
    BundlingOptions,
    Duration,
    RemovalPolicy,
    Stack,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_lambda as _lambda,
    aws_sqs as sqs,
)
from constructs import Construct

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
        self.scraped_message_queue = sqs.Queue(
            self,
            "ScrapedMessageQueue",
            removal_policy=RemovalPolicy.DESTROY,
            retention_period=Duration.days(4),
            visibility_timeout=Duration.seconds(10),  # retry failed message quickly
        )

        # stateless resources
        self.publish_message_to_sqs_lambda = _lambda.Function(
            self,
            "PublishMessageToSQSLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/publish_message_to_sqs_lambda",
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
            timeout=Duration.seconds(5),  # should be fairly quick
            memory_size=128,  # in MB
            environment={"AWSREGION": environment["AWS_REGION"]},
        )

        # connect AWS resources
        self.scheduled_eventbridge_event.add_target(
            target=events_targets.LambdaFunction(
                handler=self.publish_message_to_sqs_lambda,
                retry_attempts=3,
                ### then put in DLQ
            ),
        )
        self.scraped_message_queue.grant_send_messages(self.publish_message_to_sqs_lambda)
        self.publish_message_to_sqs_lambda.add_environment(
            key="QUEUE_NAME", value=self.scraped_message_queue.queue_name
        )

        # Store Message Stack
