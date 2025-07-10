import subprocess as sp  # nosec
import os
from aws_cdk import (
    aws_lambda as lambda_,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration,
)
from aws_cdk.aws_lambda import IFunction
from aws_cdk.aws_stepfunctions_tasks import LambdaInvoke
from constructs import Construct
from aws_cdk.aws_stepfunctions import JsonPath, TaskInput
import config as cf
import pipeline_stacks.pipeline_config as pcfg


def create_layer(
    scope: Construct,
    layer_id: str,
    makefile_path: str,
    code_asset_name: str,
) -> lambda_.LayerVersion:
    """Creates a layer and returns handle"""
    sp.call(["make", "bundle"], cwd=makefile_path)  # nosec

    layer = lambda_.LayerVersion(
        scope,
        id=layer_id,
        code=lambda_.Code.from_asset(
            os.path.join(makefile_path, code_asset_name)
        ),  # noqa
    )

    sp.call(["make", "clean"], cwd=makefile_path)  # nosec

    return layer


def get_lambda_object(
    scope: Construct, pipeline_name: str, lambda_name: str
) -> IFunction:
    lambda_object = lambda_.Function.from_function_arn(
        scope,
        id=lambda_name
        if pipeline_name == ""
        else f"{pipeline_name}-{lambda_name}",  # noqa
        function_arn=f"arn:aws:lambda:{cf.REGION}:{cf.ACCOUNT}:"
        f"function:{lambda_name}",
    )
    return lambda_object


def get_lambda_payload(
    lambda_name: str, frequency: str, pipeline_name: str
) -> TaskInput:
    payload = None
    if lambda_name in [
        cf.AUDIT_CONFIG_GEN_LAMBDA_NAME,
    ]:
        payload = sfn.TaskInput.from_object(
            {
                "payload": {
                    "pipeline_type": sfn.JsonPath.string_at("$.pipeline_type"),
                    "frequency": frequency,
                    "exec_date": sfn.JsonPath.string_at("$.date"),
                }
            }
        )
    elif lambda_name in [cf.DONE_LAMBDA_NAME]:
        payload = sfn.TaskInput.from_object(
            {
                "payload": {
                    "create_done": {
                        "pipeline_name": sfn.JsonPath.string_at(
                            "$.pipeline_type"
                        ),  # noqa
                        "exec_date": sfn.JsonPath.string_at("$.date"),
                    }
                }
            }
        )
    return payload


def get_lambda_step(
    scope: Construct, lambda_name: str, frequency: str, pipeline_name: str = ""
) -> LambdaInvoke:
    lambda_object = get_lambda_object(
        scope=scope, pipeline_name=pipeline_name, lambda_name=lambda_name
    )
    lambda_payload = get_lambda_payload(
        frequency=frequency,
        lambda_name=lambda_name,
        pipeline_name=pipeline_name,  # noqa
    )
    landing_step = tasks.LambdaInvoke(
        scope,
        f"{frequency}-{lambda_name}",
        lambda_function=lambda_object,
        payload=lambda_payload,
        result_path=JsonPath.DISCARD,
        retry_on_service_exceptions=False,
    )
    errors = sfn.Errors()
    landing_step = landing_step.add_retry(
        max_attempts=pcfg.LAMBDA_TASK_RETRY,
        interval=Duration.seconds(pcfg.WAITING_TIME_BEFORE_RETRY),
        errors=[errors.ALL],  # noqa
    )
    return landing_step
