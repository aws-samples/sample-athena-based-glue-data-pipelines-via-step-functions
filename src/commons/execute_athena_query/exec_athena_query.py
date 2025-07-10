import json
import os
import sys
from typing import Union, Any
import boto3
import logging
from urllib.parse import urlparse
from botocore.exceptions import ClientError
import awswrangler as wr
from jinja2 import StrictUndefined, FileSystemLoader
from jinja2.environment import Environment

import re

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DATA_GRAIN_PARTITIONS = {"daily": "daily", "monthly": "monthly"}

# """
from awsglue.utils import getResolvedOptions  # noqa

xtra_files_dir = os.environ["EXTRA_FILES_DIR"]

args = getResolvedOptions(
    sys.argv,
    [
        "s3_glue_asset_bucket",
        "s3_sql_script_key",
        "s3_sql_script_param_key",
        "param_execution_date",
        "glue_runtime_sql_params",
        "glue_dest_table_props",
        "glue_execution_db",
        "glue_job_name",
        "pipeline_name",
        "task_type",
        "step_execution_id",
        "start_dttm",
        "env",
        "can_fetch_no_results",
    ],
)
env = args["env"]
step_execution_id = args["step_execution_id"]
start_dttm = args["start_dttm"]
task_type = args["task_type"]
glue_job_name = args["glue_job_name"]
pipeline_name = args["pipeline_name"]
s3_glue_asset_bucket = args["s3_glue_asset_bucket"]
s3_sql_script_key = args["s3_sql_script_key"]
s3_sql_script_param_key = args["s3_sql_script_param_key"].format(
    param_exec_date=args["param_execution_date"]
)

logger.info(f"Using param config from {s3_sql_script_param_key}")

glue_runtime_sql_params = (
    json.loads(args["glue_runtime_sql_params"])
    if len(args["glue_runtime_sql_params"]) > 0
    else {}
)
s3_sql_script_path = f"s3://{s3_glue_asset_bucket}/{s3_sql_script_key}"
s3_sql_script_param_path = f"s3://{s3_glue_asset_bucket}/{s3_sql_script_param_key}"
glue_execution_db = args["glue_execution_db"]
can_fetch_no_results = args["can_fetch_no_results"]

param_execution_date = glue_runtime_sql_params["param_execution_date"]
param_landing_db_name = glue_runtime_sql_params["param_landing_db_name"]
param_processed_db_name = glue_runtime_sql_params["param_processed_db_name"]
param_s3_landing_bucket_name = glue_runtime_sql_params["param_s3_landing_bucket_name"]  # noqa

glue_dest_table_props = (
    json.loads(args["glue_dest_table_props"])
    if len(args["glue_dest_table_props"]) > 0
    else {}
)

param_paths = s3_sql_script_param_key.split(",")
s3_sql_script_param_path = []
for param_path in param_paths:
    if len(param_path.strip()) > 0:
        s3_sql_script_param_path.append(
            f"s3://{s3_glue_asset_bucket}/{param_path.strip()}"
        )

dest_table = {}
if len(glue_dest_table_props) > 0:
    """
    Overwrite data = Yes can indicate following scenarios
        1. Overwrite partition when partition is given
        2. If partition is not given, overwrites entire table data
    """
    dest_table["table_name"] = glue_dest_table_props["table_name"]
    dest_table["overwrite_data"] = (
        True if glue_dest_table_props["overwrite_data"].lower() == "yes" else False
    )
    dest_table["table_bucket"] = glue_dest_table_props["table_bucket"]
    dest_table["table_db"] = glue_dest_table_props["table_db"]
    dest_table["table_partition"] = glue_dest_table_props["table_partition"]

else:
    logger.info("glue_dest_table_props is missing")

max_rows_per_file_s3 = 50000
rendered_s3_sql_path = ""


def get_objects_in_s3_path(bucket_name: str, bucket_path: str) -> list:
    print("In get_objects_in_s3_path...")
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=bucket_path)
    s3_objects = []
    for page in pages:
        try:
            for obj in page["Contents"]:
                s3_objects.append(obj["Key"])
        except KeyError:
            logging.info(f"No items in s3 bucket {bucket_name} in path {bucket_path}")

    return s3_objects


def delete_objects_from_s3_path(bucket_name: str, bucket_prefix: str) -> int:
    print("In delete_objects_from_s3_path...")
    print(
        f"In delete_objects_from_s3_path : "
        f"bucket_name = {bucket_name}, bucket_path = {bucket_prefix}"
    )

    k = 0
    if len(bucket_prefix.strip()) > 0:
        s3 = boto3.client("s3")
        objects_to_delete = get_objects_in_s3_path(
            bucket_name=bucket_name, bucket_path=bucket_prefix
        )
        for k, key in enumerate(objects_to_delete, 1):
            logger.info(f"Deleting object {k} : {key}...")
            response = s3.delete_object(Bucket=bucket_name, Key=key)
            print(response)
    print(
        f"Total objects in path s3://{bucket_name}/{bucket_prefix} to be delete : {k}"
    )
    return k


def get_s3_file_content(s3_path: str) -> str:
    print("In get_s3_file_content...")
    try:
        s3 = boto3.resource("s3")
        o = urlparse(s3_path)
        bucket = o.netloc
        key = o.path
        obj = s3.Object(bucket, key.lstrip("/"))
        file_content = obj.get()["Body"].read().decode("utf-8")
        return file_content
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchKey":
            logger.info(f"No object found:{s3_path}")
            return "{}"
        else:
            raise


def templatize_query_j2(sql_script_path: str, sql_params_path: [str]) -> (str, dict):
    logger.info("In templatize_query_j2...")
    sql = get_s3_file_content(sql_script_path)
    render_params = {}
    rendered_sql = ""

    if task_type == "audit":
        for param_path in sql_params_path:
            param = json.loads(get_s3_file_content(param_path))
            if len(param) > 0:
                render_params.update(param)

    render_params.update(glue_runtime_sql_params)
    if task_type == "audit":
        render_params["globals"]["param_exec_date"] = render_params[
            "param_execution_date"
        ]
        render_params["globals"]["param_processed_db_name"] = render_params[
            "param_processed_db_name"
        ]
        audit_meta = {
            "param_audit_db": f"audit_db_{render_params['globals']['param_stage'].lower()}",
            "param_audit_table": "audit",
        }
        j2_sql = Environment(  # nosec
            loader=FileSystemLoader(xtra_files_dir),
            undefined=StrictUndefined,
            lstrip_blocks=True,
        ).from_string(sql)
        rendered_sql = j2_sql.render(
            configs=render_params["configs"],
            globals=render_params["globals"],
            table_select_period_pattern=render_params["table_select_period_pattern"],
            table_level_where=render_params["table_level_filter"],
            audit=audit_meta,
        )
    else:
        sql = get_s3_file_content(sql_script_path)
        interested_params = {"param_execution_date": param_execution_date \
            , "param_landing_db_name": param_landing_db_name \
            , "param_processed_db_name": param_processed_db_name \
            , "param_s3_landing_bucket_name": param_s3_landing_bucket_name}  # noqa

        for key in interested_params:
            sql = sql.replace("{{ " + key + " }}", interested_params[key])
        rendered_sql = sql
    return rendered_sql, render_params


def parameterize_query(sql_script_path: str, sql_params_path: str) -> str:
    logger.info("In parameterize_query...")
    sql = get_s3_file_content(sql_script_path)
    params = get_s3_file_content(sql_params_path)
    params_dict = json.loads(params)
    params_dict.update(glue_runtime_sql_params)
    logger.info(f"The params file at {sql_params_path} is empty") if len(
        params_dict
    ) < 1 else None
    if len(sql.strip()) > 5:
        for param_key, param_value in params_dict.items():
            sql = sql.replace(param_key, param_value.strip())
    else:
        raise Exception(f"Empty SQL file : {sql_script_path}")
    return sql


def check_query_results(query_status: dict) -> bool:
    """Validate if Athena query yielded any result"""
    logger.info("In check_query_results")
    query_zero_result = (
            query_status["StatementType"].lower() == "dml"
            and query_status["Statistics"]["DataScannedInBytes"] == 0  # noqa
    )
    if query_zero_result and can_fetch_no_results:
        logger.info(f"Query {query_status['QueryExecutionId']} did not yield any result,"
                    f" but task is set not to fail in config")

    if query_zero_result and can_fetch_no_results is False:
        raise Exception(
            f"Exception: Query {query_status['QueryExecutionId']} did not yield any result"
        )
    return True


def start_query_execution(sql_qry: str) -> Union[str, dict[str, Any]]:
    logger.info("In start_query_execution")
    logger.info(
        f"Running Statement in {glue_execution_db} database: {rendered_s3_sql_path} "
    )
    query_exec_status = wr.athena.start_query_execution(
        sql=sql_qry, database=glue_execution_db, wait=True
    )
    logger.info(f"ATHENA RESPONSE start_query_execution = {query_exec_status}")
    if task_type != "audit":
        check_query_results(query_exec_status)
    return query_exec_status


def s3_upload_file(dest_bucket: str, dest_prefix: str, content: str) -> str:
    print("In s3_upload_file...")
    rendered_sql_upload_path = f"s3://{dest_bucket}/{dest_prefix}"
    s3 = boto3.client("s3")
    s3.put_object(Bucket=dest_bucket, Key=dest_prefix, Body=content)
    return rendered_sql_upload_path


def get_data_grain_partition():
    # DATA_GRAIN_PARTITIONS
    if pipeline_name.lower().find("tank") >= 0:
        if glue_runtime_sql_params["monthly"].lower() == "true":
            return [DATA_GRAIN_PARTITIONS["daily"], DATA_GRAIN_PARTITIONS["monthly"]]
        else:
            return [DATA_GRAIN_PARTITIONS["daily"]]
    else:
        raise Exception("invalid partitions in get_data_grain_partition")


def update_partition_values(partitions: dict, params: dict) -> (dict, bool):
    logger.info("In update_partition_values...")
    multiple_sub_partitions = False
    for partition in partitions:
        if task_type == "audit":
            if "pipeline" in partition:
                partitions["pipeline"] = params["globals"]["param_pipeline_name"]
            if "exec_date" in partition:
                partitions["exec_date"] = params["globals"]["param_exec_date"]
            if "table_name" in partition:
                partitions["table_name"] = params["globals"]["param_audited_table_name"]
            if "time_grain" in partition:
                partitions["time_grain"] = params["globals"]["param_grain"]
        else:
            if "exec_date" in partition:
                partitions["exec_date"] = params["param_execution_date"]
            if "data_grain" in partition:
                partitions["data_grain"] = get_data_grain_partition()
                multiple_sub_partitions = True

    return partitions, multiple_sub_partitions


def construct_partition_path(partitions: dict, params: dict) -> list:
    logger.info("In construct_partition_path...")
    partition_path = []
    prefix = ""
    partitions_with_value, is_sub_partitions = update_partition_values(
        partitions=partitions, params=params
    )
    # for partition in partitions_with_value.items():
    if task_type == "data-transform":
        for partition_key, partition_value in partitions_with_value.items():
            if is_sub_partitions:
                if type(partition_value).__name__ == "list":
                    for sub_part in partition_value:
                        partition_path.append(f"{prefix}{partition_key}={sub_part}/")
                else:
                    prefix = f"{partition_key}={partition_value}/"
            else:
                partition_path.append(f"{partition_key}={partition_value}/")
    elif task_type == "audit":
        audit_partition_path = ""
        for partition_key, partition_value in partitions_with_value.items():
            audit_partition_path = (
                    audit_partition_path + f"{partition_key}={partition_value}/"
            )
        partition_path.append(audit_partition_path)
    return partition_path


def read_sql_query(sql_qry_select):
    print("In read_sql_query")
    df_temp = wr.athena.read_sql_query(sql_qry_select, database=glue_execution_db)  # noqa
    return df_temp


def compact_and_write_to_parquet(df_selected):
    print("In compact_and_write_to_parquet")
    write_mode = (
        "overwrite_partitions"
        if glue_dest_table_props["overwrite_data"].lower() == "yes"
        else "append"
    )
    list_of_files_output = wr.s3.to_parquet(
        df=df_selected,
        dataset=True,
        partition_cols=list(dest_table["table_partition"].keys()),
        max_rows_by_file=max_rows_per_file_s3,
        mode=write_mode,
        database=dest_table["table_db"],
        table=dest_table["table_name"],
    )
    return list_of_files_output


def check_potential_sql_injection_patterns(value: str) -> bool:
    """Check for suspicious SQL patterns."""
    dangerous_patterns = [
        r'DROP\s+TABLE',  # DROP attacks
        r'DELETE\s+FROM',  # DELETE attacks
        r'EXEC\s*\(',  # EXEC attacks
        r'EXECUTE\s*\(',  # EXECUTE attacks
        r'xp_cmdshell',  # Command execution
        r'sys\.',  # System table access
        r'information_schema\.',  # Information schema access
    ]

    return any(re.search(pattern, value, re.IGNORECASE)
               for pattern in dangerous_patterns)


def default_exec_sql(sql_qrys: str, render_params: dict) -> Union[str, dict[str, Any]]:
    logger.info("In default_exec_sql")
    exec_summary = None

    def clean_up_partition():
        logger.info("In default_exec_sql -> clean_up_partition")
        logger.info(
            "Destination table exists and query execution "
            "will insert and overwrite with new data"
        )
        table_paths = []
        if len(dest_table["table_partition"]) > 0:
            # fmt: off
            if (task_type == 'data-transform' and len(dest_table['table_partition']) <= 2) or \
                    (task_type == 'audit' and len(dest_table['table_partition']) == 4):
                partition_path = construct_partition_path(partitions=dest_table['table_partition'],  # noqa
                                                          params=render_params)
                # fmt: on
                for partition in partition_path:
                    table_paths.append(
                        f"{dest_table['table_db']}/{dest_table['table_name']}/{partition}"
                    )
            else:
                raise Exception(
                    f"Received table = {glue_dest_table_props['table_name']} with "
                    f"task type = {task_type} and "
                    f"{len(dest_table['table_partition'])} partitions ="
                    f" {dest_table['table_partition']} \n"
                    f"Currently partitions for data-transform can be max 2 levels "
                    f"and audit can be 4 levels"
                )
        else:
            table_paths.append(f"{dest_table['table_db']}/{dest_table['table_name']}")

        for table_path in table_paths:
            d = delete_objects_from_s3_path(
                bucket_name=dest_table["table_bucket"], bucket_prefix=table_path
            )
            logger.info(
                f"Purged : {d} objects deleted in table path "
                f"s3://{dest_table['table_bucket']}/{table_path}"
            )

    if "overwrite_data" in dest_table:
        if dest_table["overwrite_data"]:
            clean_up_partition()

    for sql_query in sql_qrys.split(";"):
        sql_query = sql_query.strip()
        if sql_query != "":
            if check_potential_sql_injection_patterns(sql_query):
                raise Exception(
                    f"Potential SQL injection detected in query: {sql_query}"
                )

            exec_summary = start_query_execution(sql_query)
            logger.info(
                f"Execution Summary : \n"
                f"EXECUTION ID  : {exec_summary['QueryExecutionId']}  \n"
                f"STATUS        : {exec_summary['Status']} \n"
                f"STATISTICS    : {exec_summary['Statistics']} \n"
            )

    return exec_summary


def exec_sql(
        sql_qrys: str, render_params: dict, compact_results: bool = False
) -> Union[str, dict[str, Any]]:
    return default_exec_sql(sql_qrys=sql_qrys, render_params=render_params)


def exec_athena_script(sql_script_path: str, sql_params_path: [str]):
    logger.info("In exec_athena_script...")
    try:
        template_rendered_query, render_params = templatize_query_j2(
            sql_script_path=sql_script_path, sql_params_path=sql_params_path
        )

        upload_rendered_sql_path = ""
        # fmt: off
        step_exec_id = step_execution_id[step_execution_id.rfind(":") + 1:len(step_execution_id)]  # noqa
        # fmt: on
        path_prefix_1 = f"pipeline_executions/{pipeline_name}"
        path_prefix_2 = f"{start_dttm}-{step_exec_id}/" f"{glue_job_name}_rendered.sql"
        if task_type == "audit":
            upload_rendered_sql_path = f"{path_prefix_1}/{render_params['globals']['param_exec_date']}/{path_prefix_2}"  # noqa
        else:
            upload_rendered_sql_path = f"{path_prefix_1}/{param_execution_date}/{path_prefix_2}"  # noqa
        rendered_s3_sql_path = s3_upload_file(  # noqa
            dest_bucket=s3_glue_asset_bucket,
            dest_prefix=upload_rendered_sql_path,
            content=template_rendered_query,
        )

        exec_summary = exec_sql(  # noqa
            template_rendered_query,
            render_params=render_params,
        )
    except Exception as e:
        logger.debug(e)
        raise


exec_athena_script(s3_sql_script_path, s3_sql_script_param_path)
