"""Module that contains generalized glue resource and permission routines"""

import json
from typing import Dict, List, Optional, Tuple

from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    Fn,
    aws_s3 as s3,
    aws_logs as logs,
)
from aws_cdk.aws_logs import RetentionDays
from constructs import Construct

import pipeline_stacks.pipeline_config as pcfg
from pkg import iam_helpers, s3_helpers

import config as cf

read_key_arn = Fn.import_value(cf.GLUE_JOBS_KEY_ARN)
GLUE_LOGS_POLICY_STATEMENT = iam.PolicyStatement(
    actions=[
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:AssociateKmsKey",
    ],
    effect=iam.Effect.ALLOW,
    resources=[
        "arn:aws:logs:*:*:/aws-glue/*",
        "arn:aws:logs:*:*:log-group:/aws-glue/python-jobs/*/*",
        read_key_arn,
    ],
    sid="LogsAccess",
)


def create_glue_crawler(
    scope: Construct,
    database_name: str,
    bucket_type: str,
    table_name: str = "*",
    is_schema_change_delete: bool = True,
    is_schema_change_update: bool = True,
    is_combine_compatible_schemas: bool = True,
    is_inherit_table_schema: bool = True,
    schedule: Optional[str] = None,
    file_exclusions: Optional[List[str]] = None,
    role: Optional[iam.Role] = None,
    role_name: Optional[str] = None,
) -> Tuple[glue.CfnCrawler, iam.Role]:
    """General routine to create glue crawler and role

    scope: cdk.Construct for lineage
    database_name: name of glue database_name and first s3 directory in source bucket # noqa
    bucket_type: 'landing', 'processed'
    table_name: Glue table name and second s3 directory in source bucket.
        If '*', then crawl all dirs in db
    is_schema_change_delete: delete partitions from table metadata when data is deleted on S3?
    is_schema_change_update: Update schema in database if new file has new schema else just log
    is_combine_compatible_schemas: Coalesce file-level schemas to table level else
        create separate tables
    is_inherit_table_schema: Whether partitions should inherit table schema
    schedule: Set crawler schedule via cron() syntax
    file_exclusions: List of strings representing path exclusions for s3 crawler
    role: Optional - Existing role used when preferable
    role_name: Optional - for user-specified role name

    Naming Convention Example:

    1) create_glue_crawler(scope, "test_database", "landing", "dynamodb_events")
        s3://LANDING_BUCKET/test_database/raw_dynamodb_events/PARTITION_1=value/...
            Glue database = "test_database"
            Glue table created by crawler = "raw_dynamodb_events"

    2) create_glue_crawler(scope, "test_database", "processed", "pmc_events")
        s3://PROCESSED_BUCKET/test_database/sanitized_dynamodb_events/PARTITION_1=value/...
            Glue database = "test_database"
            Glue table created by crawler = "sanitized_dynamodb_events"
    """
    source_bucket_name = s3_helpers.get_bucket_name(bucket_type)
    db_s3_path = f"s3://{source_bucket_name}/{database_name}"
    if table_name == "*":
        target_path = f"{db_s3_path}/"
        crawler_name = f"{database_name}-{bucket_type}-crawler"
        # Force false when crawling all tables in database; Want separate tables per table path # noqa
        is_combine_compatible_schemas = False
    else:
        target_path = f"{db_s3_path}/{table_name}/"
        crawler_name = f"{database_name}-{table_name}-crawler"

    if role is None:
        if not role_name and table_name == "*":
            role_name = f"{database_name}-{bucket_type}-GlueCrawlerRole"
        else:
            role_name = f"{database_name}-{table_name}-GlueCrawlerRole"
        role = iam.Role(
            scope,
            role_name.lower(),
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name=role_name,
        )
        role.add_to_policy(GLUE_LOGS_POLICY_STATEMENT)
    target = iam_helpers.S3TableLocation(
        bucket_type, database_name, [table_name]
    )  # noqa
    role.add_to_policy(target.get_s3_policy("read"))
    role.add_to_policy(target.get_glue_policy("write"))
    delete_behavior = (
        "DELETE_FROM_DATABASE"
        if is_schema_change_delete
        else "DEPRECATE_IN_DATABASE"  # noqa
    )
    update_behavior = "UPDATE_IN_DATABASE" if is_schema_change_update else "LOG"  # noqa
    inherit_table_schema_config = {
        "CrawlerOutput": {
            "Partitions": {
                "AddOrUpdateBehavior": "InheritFromTable",
            },
        },
    }
    combine_compatible_schema_config = {
        "Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas"},
    }

    configuration = {"Version": 1.0}
    if is_combine_compatible_schemas:
        configuration.update(combine_compatible_schema_config)
    if is_inherit_table_schema:
        configuration.update(inherit_table_schema_config)

    crawler = glue.CfnCrawler(
        scope,
        id=crawler_name,
        role=role.role_arn,
        targets=glue.CfnCrawler.TargetsProperty(
            s3_targets=[
                glue.CfnCrawler.S3TargetProperty(
                    path=target_path,
                    exclusions=file_exclusions,
                )
            ]
        ),
        schedule=(
            glue.CfnCrawler.ScheduleProperty(schedule_expression=schedule)
            if schedule
            else None
        ),
        database_name=database_name,
        description=f"Crawler for {database_name}.{table_name} table in {bucket_type} bucket",  # noqa
        configuration=json.dumps(configuration),
        name=crawler_name,
        schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
            delete_behavior=delete_behavior, update_behavior=update_behavior
        ),
    )
    return crawler, role


def _create_glue_job_role(
    scope: Construct,
    role_name: str,
    # source_locations: Optional[List[iam_helpers.S3TableLocation]],
    # target_locations: Optional[List[iam_helpers.S3TableLocation]],
    # direct_catalog_access: bool = False
) -> iam.Role:
    """Function that generalizes glue job role creation"""
    role = iam.Role(
        scope,
        role_name.lower(),
        assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        role_name=role_name,
    )
    # if source_locations:
    #     for source in source_locations:
    #         # Source Acces
    #         role.add_to_policy(source.get_s3_policy("read"))
    #         if direct_catalog_access:
    #             role.add_to_policy(source.get_glue_policy("read"))
    # if target_locations:
    #     for target in target_locations:
    #         role.add_to_policy(target.get_s3_policy("write"))
    #         if direct_catalog_access:
    #             role.add_to_policy(target.get_glue_policy("write"))
    #
    # # Glue Job Lib Access - HeadObject is not part of read for bucket.grant_read() # noqa
    # glue_lib_bucket = s3_helpers.get_bucket(scope, "glue_assets", f"{role.role_arn}-s3readaccess") # noqa
    #
    # # HeadObject is needed
    # role.add_to_policy(
    #     iam.PolicyStatement(
    #         actions=[
    #             "s3:HeadObject",
    #             "s3:GetBucketLocation",
    #             "s3:GetObject",
    #             "s3:ListBucket",
    #         ],
    #         resources=[glue_lib_bucket.bucket_arn, f"{glue_lib_bucket.bucket_arn}/*"], # noqa
    #     )
    # )
    #
    # role.add_to_policy(GLUE_LOGS_POLICY_STATEMENT)
    return role


def setup_glue_role(
    scope: Construct,
    role_name: str,
    source_locations: Optional[List[iam_helpers.S3TableLocation]],
    target_locations: Optional[List[iam_helpers.S3TableLocation]],
    direct_catalog_access: bool = False,
):
    role = _create_glue_job_role(scope, role_name)
    if source_locations:
        for source in source_locations:
            # Source Access
            role.add_to_policy(source.get_s3_policy("read"))
            if direct_catalog_access:
                role.add_to_policy(source.get_glue_policy("read"))
    if target_locations:
        for target in target_locations:
            role.add_to_policy(target.get_s3_policy("write"))
            if direct_catalog_access:
                role.add_to_policy(target.get_glue_policy("write"))

    # Glue Job Lib Access - HeadObject is not part of read for bucket.grant_read() # noqa
    glue_lib_bucket = s3_helpers.get_bucket(
        scope, "glue_assets", f"{role.role_arn}-s3readaccess"
    )  # noqa

    # HeadObject is needed
    role.add_to_policy(
        iam.PolicyStatement(
            actions=[
                "s3:HeadObject",
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:ListBucket",
            ],
            resources=[
                glue_lib_bucket.bucket_arn,
                f"{glue_lib_bucket.bucket_arn}/*",
            ],  # noqa
        )
    )

    role.add_to_policy(
        iam.PolicyStatement(
            actions=[
                "kms:ListKeys",
                "kms:Decrypt",
                "kms:Encrypt",
                "kms:GenerateDataKey",
                "kms:Describe*",
                "logs:AssociateKmsKey",
            ],
            resources=[read_key_arn],  # noqa
        )
    )

    role.add_to_policy(GLUE_LOGS_POLICY_STATEMENT)

    return role


def create_glue_job(
    scope: Construct,
    job_name: str,
    source_locations: Optional[List[iam_helpers.S3TableLocation]] = None,
    target_locations: Optional[List[iam_helpers.S3TableLocation]] = None,
    direct_catalog_access: bool = False,
    timeout_mins: int = 60,
    number_of_workers: int = 2,
    worker_type: str = "G.1X",
    max_concurrent_runs: int = 100,
    job_type: str = "glueetl",
    default_args: Optional[Dict] = None,
    pythonshell_dpu: float = 0.0625,
    reuse_iam_role: bool = False,
    glue_job_iam_role: iam.Role = None,
    scripts_source_bucket_name=None,
    script_name=None,
    file_prefix=None,
) -> Tuple[glue.CfnJob, iam.Role]:
    """Function that generalizes glue job and role creation

    :param target_locations:
    :param pythonshell_dpu:
    :param default_args:
    :param scope: cdk construct to create glue job under
    :param job_name: name of Glue Job
    :param source_locations: Optional source table locations
    :param target_locations: Optional target location
    :param local_script_path: local path of Glue Script
    :param direct_catalog_access: True if glue job needs access to glue data catalog # noqa
    :param timeout_mins: default timeout of glue job
    :param number_of_workers: 2 or more for glueetl jobs
    :param worker_type: "G.1X" or "G.2X"
    :param max_concurrent_runs: number of concurrent jobs to be run
    :param job_type: "glueetl" or "pythonshell"
    :param reuse_iam_role: True or False
    :param glue_job_iam_role: iam.Role for the Glue job
    :param scripts_source_bucket_name: Source bucket to hold the Job scripts
    :param script_name: The script file name
    :param file_prefix: S3 prefix to store the Glue Job script file pattern(script filename with no ext)
    """
    if reuse_iam_role:
        role = glue_job_iam_role
    else:
        role = setup_glue_role(
            scope,
            f"{job_name}_GlueJobRole",
            source_locations,
            target_locations,
            direct_catalog_access=direct_catalog_access,
        )

    destination_key_prefix = "glue_job_scripts/" + file_prefix
    scriptsHolderB = s3.Bucket.from_bucket_name(
        scope, f"{job_name}-scriptSourceBucket", scripts_source_bucket_name
    )

    security_configuration_name = Fn.import_value(cf.GLUE_JOBS_SECURITY_CONFIG_NAME)
    description = f"Glue job for {job_name}"

    glue_job = glue.CfnJob(
        scope,
        id=job_name,
        command=glue.CfnJob.JobCommandProperty(
            name=job_type,
            python_version="3.9",
            script_location=scriptsHolderB.s3_url_for_object(
                destination_key_prefix + script_name
            ),
        ),
        role=role.role_arn,
        description=description,
        glue_version=("4.0" if job_type == "glueetl" else "3.0"),
        name=job_name,
        default_arguments=default_args,
        number_of_workers=(
            number_of_workers if job_type == "glueetl" else None
        ),  # noqa
        timeout=timeout_mins,
        worker_type=(worker_type if job_type == "glueetl" else None),
        execution_property=glue.CfnJob.ExecutionPropertyProperty(
            max_concurrent_runs=max_concurrent_runs
        ),
        max_capacity=pythonshell_dpu if job_type == "pythonshell" else None,
        max_retries=pcfg.GLUE_JOBS_RETRY,
        # TODO:
        # Check with Security consultant as the SecurityConfiguration class is in an alpha package and is Experimental # noqa
        security_configuration=security_configuration_name,
    )
    return glue_job, role


def create_glue_job_cw_log_group(scope: Construct, job_name: str, role_name: str):
    security_configuration_name = Fn.import_value(cf.GLUE_JOBS_SECURITY_CONFIG_NAME)

    def create_cw_log_group(log_type: str):
        logs.LogGroup(
            scope,
            f"{job_name}-{log_type}-CWLogs",
            log_group_name=f"/aws-glue/python-jobs/{security_configuration_name}-role/{role_name}/{log_type}",  # noqa
            retention=RetentionDays.TWO_MONTHS,
        )

    create_cw_log_group(log_type="output")
    create_cw_log_group(log_type="error")
