"""
Given a table name it generates the config file that
will be used for J2 templatization
"""
import json
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse

import config
from common.log_utils import setup_logger


def handler(event, context):
    """
    Lambda function is responsible for the following,
        1. Creating table level json file for J2 string
        2. Reads configuration for table
            2.1 Fetches the configuration
            2.2 creates one SQL for all the configuration
    """
    print(f"Payload = {event}")
    ex = GenerateAuditTablesConfigJSON(event=event, context=context, cnf=config)
    return ex.execute()


class GenerateAuditTablesConfigJSON(object):
    def __init__(self, event, context, cnf):
        self.log = setup_logger()
        self.event = event
        self.context = context
        self.cnf = cnf
        self.stage = cnf.STAGE
        self.s3 = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        self.templatized_sql = ""
        self.generate_config_for_tables = []

    def get_s3_file_content(self, s3_path: str) -> str:
        try:
            o = urlparse(s3_path)
            bucket = o.netloc
            key = o.path
            obj = self.s3_resource.Object(bucket, key.lstrip("/"))
            file_content = obj.get()["Body"].read().decode("utf-8")
            return file_content
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchKey":
                self.log.info(f"No object found:{s3_path}")
                return "{}"
            else:
                raise

    def create_monthly_results_config(
        self, table_config: dict, table_s3_config: dict
    ) -> dict:
        table_template = []
        global_config = {}

        global_config["param_month_column_name"] = table_config[
            "param_month_column_name"
        ]
        global_config["param_pipeline_name"] = table_config["param_pipeline_name"]
        global_config["param_grain"] = table_config["param_grain"]

        global_config["param_exec_date"] = table_config["param_exec_date"]
        global_config["param_audited_table_name"] = table_config[
            "param_audited_table_name"
        ]
        global_config["sql_template_path"] = (
            f"s3://{self.cnf.S3_GLUE_ASSET_BUCKET}"
            f"/{self.cnf.GENERATED_TABLE_CONFIG_S3_PREFIX}"
            f"/{table_config['param_audited_table_name']}.sql"
        )
        global_config["param_layer"] = table_config["param_layer"]
        global_config["param_stage"] = table_config["param_stage"]

        global_config["param_published_db_name"] = config.PUBLISHED_DB_NAME

        if "param_monthly_results_table_name" in table_config:
            global_config["param_monthly_results_table_name"] = table_config[
                "param_monthly_results_table_name"
            ]

        for attribute in table_config["audited_attributes"]:
            temp_table_s3_config = table_s3_config.copy()
            temp_table_s3_config["render_params"]["param_audited_attribute"] = attribute
            table_template.append(json.loads(json.dumps(temp_table_s3_config)))

        final_table_configs = {"globals": global_config, "configs": table_template}

        return final_table_configs

    def s3_upload_dict_to_file(self, dest_bucket: str, dest_prefix: str, content: str):
        self.s3.put_object(Bucket=dest_bucket, Key=dest_prefix, Body=content)

    def get_table_configs(self, payload_pipeline: str) -> dict:
        for table_configs in self.cnf.TABLE_CONFIGS:
            # fmt: off
            if table_configs["pipeline_name"].lower() == payload_pipeline.lower() and \
                    self.event["payload"]["frequency"].lower() in table_configs["frequency"][0].lower():
                # fmt: on
                return table_configs
        return {}

    def execute(self):
        """Driver module"""
        i = 0
        table_configs = self.get_table_configs(
            payload_pipeline=self.event["payload"]["pipeline_type"]
        )
        if len(table_configs) > 0:
            for table_config in table_configs["table_configs"]:
                """
                - Read config S3 file
                - Substitute config from cnf
                - store to destination
                """
                i = i + 1
                s3_table_config_template = json.loads(
                    self.get_s3_file_content(
                        f"s3://{self.cnf.S3_GLUE_ASSET_BUCKET}/"
                        f"{self.cnf.TEMPLATIZED_TABLE_CONFIG_S3_PREFIX}/"
                        f"{table_configs['pipeline_type']}/"
                        f"{table_config['param_audited_table_name']}.json"
                    )
                )
                all_table_config = self.create_monthly_results_config(
                    table_config=table_config, table_s3_config=s3_table_config_template
                )
                self.s3_upload_dict_to_file(
                    dest_bucket=f"{self.cnf.S3_GLUE_ASSET_BUCKET}",
                    dest_prefix=f"{self.cnf.GENERATED_TABLE_CONFIG_S3_PREFIX}/"
                    f"{table_configs['pipeline_type']}/"
                    f"{table_config['param_audited_table_name']}.json",
                    content=json.dumps(all_table_config),
                )
                self.log.info(
                    f"Configuration created for table "
                    f"{table_config['param_audited_table_name']}"
                    f" in s3://{self.cnf.S3_GLUE_ASSET_BUCKET}/"
                    f"{self.cnf.GENERATED_TABLE_CONFIG_S3_PREFIX}/"
                    f"{table_configs['pipeline_type']}/"
                    f"{table_config['param_audited_table_name']}.json"
                )
                self.log.info(
                    f"Templatization configurations created for {i} table(s) "
                )

        else:
            self.log.error("verify audit configs have valid names : table_configs['pipeline_name'] and ['frequency'] and ['pipeline_type']")
            raise Exception("verify audit configs have valid names : table_configs['pipeline_name'] and ['frequency'] and ['pipeline_type']")


if __name__ == "__main__":
    """Run Lambda Function locally"""
    # from pprint import pprint as pp

    PAYLOAD = {
        "payload": {
            "pipeline_type": "UsGhgEmissionCalculation",
            "frequency": "daily_and_monthly",
        }
    }
    handler(event=PAYLOAD, context={})
