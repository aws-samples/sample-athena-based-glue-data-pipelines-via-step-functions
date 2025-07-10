""" Creates Views in batches
USAGE: FOR DDLs ONLY

       ***  Do not run insert commands as the script is set  ***
       ***  to execute sql stms and will create              ***
       ***  duplicate date set on each run                   ***
"""
import json
import os
import sys
from typing import Union, Any

import boto3
import logging
from botocore.exceptions import ClientError
from urllib.parse import urlparse
import awswrangler as wr

from jinja2 import StrictUndefined, FileSystemLoader
from jinja2.environment import Environment, Template

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

s3 = boto3.client("s3")
s3_resource = boto3.resource("s3")
# '''
from awsglue.utils import getResolvedOptions  # noqa

xtra_files_dir = os.environ["EXTRA_FILES_DIR"]

args = getResolvedOptions(
    sys.argv,
    [
        "s3_glue_asset_bucket",
        "s3_sql_template_key",
        "s3_sql_config_key",
        "param_execution_date",
        "glue_runtime_sql_params",
        "glue_job_name",
        "pipeline_name",
        "batch_type",
        "step_execution_id",
        "start_dttm",
        "env",
    ],
)

env = args["env"]
step_execution_id = args["step_execution_id"]
start_dttm = args["start_dttm"]
batch_type = args["batch_type"]
glue_job_name = args["glue_job_name"]
pipeline_name = args["pipeline_name"]
s3_glue_asset_bucket = args["s3_glue_asset_bucket"]
s3_sql_template_key = args["s3_sql_template_key"]
s3_sql_config_key = args["s3_sql_config_key"]

glue_runtime_sql_params = (
    json.loads(args["glue_runtime_sql_params"])
    if len(args["glue_runtime_sql_params"]) > 0
    else {}
)
s3_sql_template_path = f"s3://{s3_glue_asset_bucket}/{s3_sql_template_key}"
s3_sql_config_path = f"s3://{s3_glue_asset_bucket}/{s3_sql_config_key}"

param_execution_date = args["param_execution_date"]

# fmt: off
def get_s3_file_content(s3_path: str) -> str:
    logger.info("In get_s3_file_content module")
    try:
        o = urlparse(s3_path)
        bucket = o.netloc
        key = o.path
        obj = s3_resource.Object(bucket, key.lstrip('/'))
        file_content = obj.get()['Body'].read().decode('utf-8')
        return file_content
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            logger.info(f'No object found:{s3_path}')
            return "{}"
        else:
            raise


def get_objects_in_s3_path(bucket_name: str, bucket_path: str, file_extn: str) -> list:
    logger.info("In get_objects_in_s3_path module")
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=bucket_path)
    s3_objects = []
    for page in pages:
        try:
            for obj in page['Contents']:
                if obj['Key'].lower().find(file_extn.lower()) > 1:
                    s3_objects.append(obj['Key'])
        except KeyError:
            logger.error(f"No items in s3 bucket {bucket_name} in path {bucket_path}")
    return s3_objects


def s3_upload_dict_to_file(dest_bucket: str, dest_prefix: str, content: str):
    logger.info("In s3_upload_dict_to_file module")
    s3.put_object(
        Bucket=dest_bucket,
        Key=dest_prefix,
        Body=content
    )


def render_j2sql(j2_sql_template: Template, **kwargs) -> str:
    logger.info("In render_j2sql module")
    sql = j2_sql_template.render(kwargs)
    return sql


def render_landing_data_validation_sql(j2_sql_tmpl: Template, j2_config: dict) -> str:
    logger.info("In render_landing_data_validation_sql module")
    sql = render_j2sql(j2_sql_template=j2_sql_tmpl,
                       render_params=j2_config["render_params"],
                       globals=j2_config["globals"],
                       )
    return sql


def start_query_execution(sql_qry: str) -> Union[str, dict[str, Any]]:
    logger.info("In start_query_execution")
    logger.info(f"Running Statement : {sql_qry} ")
    query_exec_summary = wr.athena.start_query_execution(
        sql=sql_qry,
        wait=True)
    return query_exec_summary


def templatize_query_j2(j2_sql: str, sql_params_path: [str]) -> (str, dict):
    logger.info("In templatize_query_j2 module")
    render_params = {}
    rendered_sql = ""

    for param_path in sql_params_path:
        param = json.loads(get_s3_file_content(param_path))
        if len(param) > 0:
            render_params.update(param)

        render_params['globals']['param_exec_date'] = param_execution_date
        render_params.update(glue_runtime_sql_params)
        j2_sql_template = Environment(  # nosec
            loader=FileSystemLoader(xtra_files_dir),
            undefined=StrictUndefined,
            lstrip_blocks=True,
        ).from_string(j2_sql)

        if batch_type == 'landing_data_validation':
            rendered_sql = render_landing_data_validation_sql(j2_sql_template, render_params)

    return rendered_sql, render_params


try:
    step_exec_id = step_execution_id[step_execution_id.rfind(":") + 1:len(step_execution_id)]
    sql_template_path = f"s3://{s3_glue_asset_bucket}/{s3_sql_template_key}"
    rendered_sql_s3_path = f"pipeline_executions/{pipeline_name}/{param_execution_date}/" \
                           f"{start_dttm}-{step_exec_id}"
    sql_j2_template = get_s3_file_content(sql_template_path)
    config_list = get_objects_in_s3_path(bucket_name=s3_glue_asset_bucket,
                                         bucket_path=s3_sql_config_key,
                                         file_extn=".json")
    i = 0
    for i, j2_config in enumerate(config_list, start=1):  # noqa
        j2_config_path = f"s3://{s3_glue_asset_bucket}/{j2_config}"
        sql_template = sql_j2_template
        # render template
        sql_to_exec, render_params = templatize_query_j2(j2_sql=sql_template,
                                                         sql_params_path=[j2_config_path])
        # store rendered sql
        dest_path = f"{rendered_sql_s3_path}/{rendered_sql_prefix}_" \
                    f"{render_params['render_params']['param_table_name']}.sql"
        s3_upload_dict_to_file(dest_bucket=f"{s3_glue_asset_bucket}",
                               dest_prefix=dest_path,
                               content=sql_to_exec)
        # exec template
        query_exec_summary = start_query_execution(sql_to_exec)

        logger.info(f"Batch project : {batch_type}, \n"
                    f"executed sql template {sql_template_path}, \n"
                    f"using config from {j2_config_path}. \n"
                    f"Rendered sql in {dest_path}\n"
                    f"Athena query execution summary = {query_exec_summary}\n")
        sql_template = ""
        sql_to_exec = ""

    logger.info(f"Templatization configurations created for {i} views(s) ")
    if i == 0:
        raise Exception("No data validation views created")
except KeyError as e:
    logger.error(e)

# fmt: on
