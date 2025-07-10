import json
import os

from aws_cdk import aws_stepfunctions as sfn, aws_stepfunctions_tasks as tasks

from aws_cdk.aws_glue import CfnJob
from aws_cdk.aws_iam import Role
from aws_cdk.aws_logs import RetentionDays
from aws_cdk.aws_stepfunctions import IntegrationPattern, JsonPath
from aws_cdk.aws_stepfunctions_tasks import GlueStartJobRun
from aws_cdk import Duration
from constructs import Construct

import config as cf
from pipeline_stacks import pipeline_config as pipe_cfg
from pkg import glue_helpers

PATH_COMMON_SRC = os.path.join(cf.PATH_SRC, "commons")
ATHENA_QUERY_EXEC_PATH = os.path.join(PATH_COMMON_SRC, "execute_athena_query")


def create_ef_glue_job(
        scope: Construct,
        pipeline_name: str,
        device_type: str,
        table_name: str,
        db_name: dict,
        frequency: str,
        job_type: str,
        task_glue_job_role: Role,
        can_fetch_no_results: bool,
) -> CfnJob:
    # DEFAULTS
    s3_sql_script_key = ""
    table_bucket = ""
    s3_sql_script_param_key = ""
    extra_py_files = ""
    dest_table_props = "{}"
    task_type = ""
    ef_glue_job: CfnJob = None

    # exec_db logic
    if db_name["table_db"] == cf.LANDING_DB_NAME:
        table_bucket = cf.S3_LANDING_BUCKET
    elif db_name["table_db"] == cf.PROCESSED_DB_NAME:
        table_bucket = cf.S3_PROCESSED_BUCKET
    elif db_name["table_db"] == cf.AUDIT_DB_NAME:
        table_bucket = cf.S3_AUDIT_BUCKET

    if job_type == "job":
        # Logic for s3_sql_script_key
        s3_sql_script_key = (
            f"execute-athena-scripts/usghgemission/{table_name}.sql"  # noqa
        )

        # Logic for s3_sql_script_param_key
        s3_sql_script_param_key = "None"  # noqa
        # Logic for extra_py_files
        extra_py_files = (
            f"s3://{cf.S3_GLUE_ASSETS_BUCKET}/{pipe_cfg.JINJA2_WHL_S3_PREFIX},"
            f"s3://{cf.S3_GLUE_ASSETS_BUCKET}/{pipe_cfg.WRANGLER_WHL_S3_PREFIX}"
        )
        table_partition = {"exec_date": ""}

        dest_table_props = (
            "{}"
            if table_name
            in ("update_landing_partition", "yearly_update_landing_partition")
            else json.dumps(
                {
                    "table_name": table_name,
                    "overwrite_data": "yes",
                    "table_bucket": table_bucket,
                    "table_db": db_name["table_db"],
                    "table_partition": table_partition,
                    "compact_sql_result_parquets": "yes"
                }
            )
        )

        # logic for task_type
        task_type = "data-transform"

    elif job_type == "audit":
        # Logic for s3_sql_script_key
        j2_sql_script_name = pipe_cfg.SQL_J2["all_others"]
        s3_sql_script_key = (
            f"audit/templatized_jinja2_sql/{device_type}/{j2_sql_script_name}"
        )

        # Logic for s3_sql_script_param_key
        s3_sql_script_param_key = (
            f"audit/generated_table_config/{device_type}/{table_name}.json,"
            f"audit/templatized_jinja2_sql/{device_type}/audit_config.json"
        )

        # Logic for extra_py_files
        extra_py_files = (
            f"s3://{cf.S3_GLUE_ASSETS_BUCKET}/{pipe_cfg.JINJA2_WHL_S3_PREFIX},"
            f"s3://{cf.S3_GLUE_ASSETS_BUCKET}/{pipe_cfg.WRANGLER_WHL_S3_PREFIX},"
            f"s3://{cf.S3_GLUE_ASSETS_BUCKET}/j2-macros/audit.jinja2"
        )

        # logic for dest_table_props
        dest_table_props = json.dumps(
            {
                "table_name": "audit",
                "overwrite_data": "yes",
                "table_bucket": table_bucket,
                "table_db": db_name["table_db"],
                "compact_sql_result_parquets": "yes",
                "table_partition": {
                    "pipeline": "",
                    "exec_date": "",
                    "table_name": "",
                    "time_grain": "",
                },
            }
        )

        # logic for task_type
        task_type = "audit"

    # logic for  job_max_concurrent_runs
    job_max_concurrent_runs = 1

    job_name = (
        f"{device_type[:2]}{device_type[-2:]}-{job_type[:1]}-{table_name}-{frequency}"[
            :75
        ]
    )  # limit to 80 char length - glue limitation

    script_name = cf.S3_ATHENA_QUERY_FILE_NAME
    file_prefix = script_name.replace(".py", "/")
    ef_glue_job, _ = glue_helpers.create_glue_job(
        scope,
        job_name=job_name,
        timeout_mins=15,
        max_concurrent_runs=job_max_concurrent_runs,
        job_type="pythonshell",
        default_args={
            "--s3_glue_asset_bucket": cf.S3_GLUE_ASSETS_BUCKET,
            "--s3_sql_script_key": s3_sql_script_key,
            "--s3_sql_script_param_key": s3_sql_script_param_key,
            "--glue_runtime_sql_params": '{"exec_date":""}',
            "--glue_dest_table_props": dest_table_props,
            "--param_execution_date": "",
            "--glue_execution_db": db_name["exec_db"],
            "--extra-py-files": extra_py_files,
            "--pipeline_name": pipeline_name,
            "--glue_job_name": job_name,
            "--task_type": task_type,
            "--step_execution_id": "default-exec-id",
            "--start_dttm": "default-dttm",
            "--can_fetch_no_results": can_fetch_no_results,
        },
        reuse_iam_role=True,
        glue_job_iam_role=task_glue_job_role,
        scripts_source_bucket_name=cf.S3_GLUE_ASSETS_BUCKET,
        pythonshell_dpu=0.0625,
        script_name=script_name,
        file_prefix=file_prefix,
    )
    return ef_glue_job


def get_glue_steps(
        scope: Construct,
        pipeline_tasks: list,
        frequency: str,
        device_type: str,
        pipeline_name: str,
        task_glue_job_role: Role,
) -> (list[tasks.GlueStartJobRun], tasks.GlueStartJobRun):
    stage_steps = []
    ef_glue_step: GlueStartJobRun = None
    for task in pipeline_tasks:
        for table in task["tables"]:
            ef_glue_job = create_ef_glue_job(
                scope,
                pipeline_name=pipeline_name,
                table_name=table,
                db_name=task["db_name"],
                frequency=frequency,
                job_type=task["function"],
                device_type=device_type,
                task_glue_job_role=task_glue_job_role,
                can_fetch_no_results=task['can_fetch_no_results'] if 'can_fetch_no_results' in task.keys() else False
            )
            ef_glue_step = create_glue_step(
                scope, glue_job_name=ef_glue_job.name
            )  # noqa
            stage_steps.append(ef_glue_step)

    return_step = ef_glue_step if len(stage_steps) == 1 else stage_steps
    return return_step


def create_parallel_snf_definition(
        scope: Construct, steps: list, name: str
) -> sfn.Parallel:
    parallel_step = sfn.Parallel(scope, name, result_path=JsonPath.DISCARD)
    for step in steps:
        parallel_step.branch(sfn.Chain.start(step))
    return parallel_step


def create_glue_step(
        scope: Construct, glue_job_name: str
) -> tasks.GlueStartJobRun:  # noqa
    glue_step = tasks.GlueStartJobRun(
        scope,
        f"{glue_job_name}-step",
        glue_job_name=glue_job_name,
        integration_pattern=IntegrationPattern.RUN_JOB,
        arguments=sfn.TaskInput.from_object(
            {
                "--param_execution_date.$": "$.date",
                "--pipeline_type.$": "$.pipeline_type",
                "--glue_runtime_sql_params.$": "$.glue_runtime_sql_params",
                "--step_execution_id.$": "$$.Execution.Id",
                "--start_dttm.$": "$.start_dttm",
                "--env.$": "$.env",
            }
        ),
        result_path=JsonPath.DISCARD,
    )
    errors = sfn.Errors()
    glue_step = glue_step.add_retry(
        max_attempts=pipe_cfg.GLUE_TASK_RETRY,
        interval=Duration.seconds(pipe_cfg.WAITING_TIME_BEFORE_RETRY),
        errors=[errors.ALL],  # noqa
    )
    return glue_step
