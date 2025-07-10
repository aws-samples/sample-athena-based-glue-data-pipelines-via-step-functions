"""Base Pipeline Construct for all pipelines in data lake"""

import os
from aws_cdk import (
    aws_lambda as lambda_,
    aws_iam as iam,
    Duration,
    aws_s3_notifications,
    aws_events as events,
    aws_s3 as s3,
    Stack,
    aws_events_targets as targets
)
from aws_cdk.aws_iam import Policy
import config as cf
# from aws_cdk import Stack
from constructs import Construct
# import aws_cdk.aws_events as events
# import aws_cdk.aws_events_targets as targets


class BasePipeline(Stack):
    """Construct containing resources for base pipeline"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """Create construct"""
        super().__init__(scope, construct_id, **kwargs)

        path_common_src = os.path.join(cf.PATH_SRC, "commons")
        path_wf_trigger_src = os.path.join(cf.PATH_SRC, "workflow_trigger_lambda")

        # Ingestion : Workflow Trigger Lambda
        lambda_timeout_seconds = 900

        wf_trigger_lambda = lambda_.Function(
            self,
            id="apg-workflow-trigger-lambda",
            handler="workflow_trigger.handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset(
                path=path_wf_trigger_src, exclude=["create_done_file.py"]
            ),
            function_name="apg-workflow-trigger-lambda",
            environment={
                "STAGE": cf.DEPLOYMENT_STAGE,
                "REGION": cf.REGION,
                "ACCOUNT": cf.ACCOUNT,
            },
            memory_size=500,
            timeout=Duration.seconds(lambda_timeout_seconds),
        )

        landing_bucket = s3.Bucket.from_bucket_name(
            self, f"imported-bucket-{cf.S3_LANDING_BUCKET}", cf.S3_LANDING_BUCKET
        )

        landing_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            aws_s3_notifications.LambdaDestination(wf_trigger_lambda),
            s3.NotificationKeyFilter(
                prefix=f"{cf.S3_LANDING_INCOMING_PATH}/",
                suffix=".done",
            ),
        )

        wf_trigger_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:PutObject", "s3:Get*", "s3:List*", "s3:DeleteObject"],
                resources=[
                    f"arn:aws:s3:::{cf.S3_LANDING_BUCKET}/*",
                    f"arn:aws:s3:::{cf.S3_LANDING_BUCKET}",
                    f"arn:aws:s3:::{cf.S3_PROCESSED_BUCKET}/*",
                    f"arn:aws:s3:::{cf.S3_PROCESSED_BUCKET}",
                ],
            )
        )

        wf_trigger_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["states:StartExecution", "states:ListExecutions"],
                resources=[f"arn:aws:states:*:{cf.ACCOUNT}:stateMachine:*"],
            )
        )

        wf_trigger_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "ssm:GetParameterHistory",
                    "ssm:GetParametersByPath",
                    "ssm:GetParameters",
                    "ssm:GetParameter",
                ],
                resources=[f"arn:aws:ssm:{cf.REGION}:{cf.ACCOUNT}:parameter/*"],
            )
        )

        wf_trigger_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:PutObject", "s3:GetObject"],
                resources=[
                    f"arn:aws:s3:::{cf.S3_GLUE_ASSETS_BUCKET}/{cf.S3_GLUE_ASSETS_STAGE}/*",  # noqa
                ],
            )
        )

        # Lambda : Create .DONE file based on ingested files
        create_done_file_lambda = lambda_.Function(
            self,
            id=f"{cf.DONE_LAMBDA_NAME}",
            handler="create_done_file.handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset(
                path=path_wf_trigger_src, exclude=["workflow_trigger.py"]
            ),
            function_name="apg-create-done-file-lambda",
            environment={
                "STAGE": cf.DEPLOYMENT_STAGE,
                "REGION": cf.REGION,
                "ACCOUNT": cf.ACCOUNT,
            },
            memory_size=128,
            timeout=Duration.seconds(600),
        )

        create_done_file_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:PutObject", "s3:Get*", "s3:List*"],
                resources=[
                    f"arn:aws:s3:::{cf.S3_LANDING_BUCKET}/incoming/*",
                    f"arn:aws:s3:::{cf.S3_LANDING_BUCKET}",
                ],
            )
        )

        # Event Bridge rule to schedule lambda.Function
        rule_trigger_done_file_lambda = events.Rule(
            self,
            "Schedule Done File Lambda",
            schedule=events.Schedule.cron(minute="0", hour="*"),
        )
        rule_trigger_done_file_lambda.add_target(
            targets.LambdaFunction(create_done_file_lambda)
        )

        s3_glue_assets_bucket_perm = Policy(
            self,
            id="s3-glue-assets-bucket-permissions",
            policy_name="s3-glue-assets-bucket-permissions",
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:PutObject",
                            "s3:Get*",
                            "s3:List*",
                            "s3:DeleteObject",
                        ],
                        resources=[
                            f"arn:aws:s3:::{cf.S3_GLUE_ASSETS_BUCKET}/*",
                            f"arn:aws:s3:::{cf.S3_GLUE_ASSETS_BUCKET}",
                        ],
                    )
                ]
            ),
        )

        ############################################
        #        AUDIT : CONFIG GENERATOR
        ############################################

        path_audit_config_generator_src = os.path.join(
            path_common_src, "sql_templatize/audit_table_config_generator"
        )

        # Lambda responsible for creating Jinja2 configuration file
        lambda_timeout_seconds = 900

        audit_config_generator_lambda = lambda_.Function(
            self,
            id=cf.AUDIT_CONFIG_GEN_LAMBDA_NAME,
            handler="audit_config_generator.handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset(
                path=path_audit_config_generator_src,
                exclude=["reference_table_json_templates"],
            ),
            function_name="audit-config-generator-lambda",
            environment={
                "STAGE": cf.DEPLOYMENT_STAGE,
                "REGION": cf.REGION,
                "ACCOUNT": cf.ACCOUNT,
            },
            memory_size=128,
            timeout=Duration.seconds(lambda_timeout_seconds)
        )


        audit_config_generator_lambda.role.attach_inline_policy(
            s3_glue_assets_bucket_perm
        )
        audit_config_generator_lambda.role.attach_inline_policy(
            self.lambda_execution_role_logs_permission_inline_policy_generator(
                lamda_name=cf.AUDIT_CONFIG_GEN_LAMBDA_NAME
            )  # noqa
        )

    def lambda_execution_role_logs_permission_inline_policy_generator(
        self, lamda_name: str
    ):  # noqa
        name = f"{lamda_name}-logs-policy"
        inline_policy = Policy(
            self,
            id=name,
            policy_name=name,
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents",
                        ],
                        resources=[
                            f"arn:aws:logs:{cf.REGION}:{cf.ACCOUNT}:log-group:/aws/lambda/{lamda_name}:*"  # noqa
                        ],
                    )
                ]
            ),
        )

        return inline_policy
