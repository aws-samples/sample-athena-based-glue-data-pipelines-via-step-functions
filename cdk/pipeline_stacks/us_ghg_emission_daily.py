""" Pipeline Construct for Calculating GHG Emissions """

from aws_cdk import (
    aws_stepfunctions as sfn,
    Duration,
    aws_ssm as ssm,
    Stack,
)
from aws_cdk.aws_stepfunctions import JsonPath

import config as cf
import pipeline_stacks.usghgemission_daily_config as u_cfg

from constructs import Construct

from pkg.glue_helpers import create_glue_job_cw_log_group
from pkg.glue_step_helpers import (  # noqa
    create_glue_step,
    get_glue_steps,
    create_parallel_snf_definition,
)

from pkg.common_policy import create_standard_glue_job_role
from pkg.lambda_helpers import get_lambda_step


class USGHGEmissionDailyPipeline(Stack):
    """Construct containing resources for Emission Factor pipeline"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """Create construct"""
        super().__init__(scope, construct_id, **kwargs)

        device_type = "usghgemission_daily"
        pipeline_name = cf.PIPELINE_NAME[device_type]
        ef_task_glue_job_role_name = f"{device_type}_pipeline_glue_role"

        ef_task_glue_job_role = create_standard_glue_job_role(
            scope=self, iam_role_name=ef_task_glue_job_role_name
        )

        create_glue_job_cw_log_group(
            scope=self, job_name=device_type, role_name=ef_task_glue_job_role_name
        )
        
        update_landing_partition_step = get_glue_steps(
            self,
            pipeline_tasks=u_cfg.UPDATE_LANDING_PARTITION,
            frequency="daily",
            device_type=device_type,
            task_glue_job_role=ef_task_glue_job_role,
            pipeline_name=pipeline_name,
        )

        UTILITY_EMISSION_DAILY = get_glue_steps(
            self,
            pipeline_tasks=u_cfg.UTILITY_EMISSION_DAILY,
            frequency="daily",
            device_type=device_type,
            task_glue_job_role=ef_task_glue_job_role,
            pipeline_name=pipeline_name,
        )

        usghg_definition = (
            sfn.Chain.start(update_landing_partition_step)
            .next(
                UTILITY_EMISSION_DAILY
                )
            .next(
                get_lambda_step(
                    self, lambda_name=cf.DONE_LAMBDA_NAME, frequency="daily"
                )
            )
        )

        comp_usghg_ef_sm = sfn.StateMachine(
            self,
            "USGHGEmissionFactorDailyWorkflow",
            definition=usghg_definition,
            timeout=Duration.minutes(30),
        )

        comp_usghg_emission_sm_name_parameter = ssm.StringParameter(  # noqa
            self,
            id="sm-usghg-emission-factor-daily",
            parameter_name="/pipeline/sm-usghg-emission-factor-daily",
            string_value=comp_usghg_ef_sm.state_machine_name,
        )
