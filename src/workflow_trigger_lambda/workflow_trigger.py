#!/usr/bin/python3
"""
The Lambda function is responsible for triggering the
respective StepFunction based on the file loaded
"""

import json
import logging
import os
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from urllib.parse import urlparse

import config as cfg
from common.log_utils import setup_logger


def handler(event, context):
    """
    Lambda function is responsible for the following,
        1. Verify if the file is valid
        2. Copy to respective input path with date in Athena friendly format
            2.1 Options to store without partition
        3. Trigger respective step function
    """

    ex = TriggerStateMachine(event=event, context=context, cnf=cfg)
    return ex.execute()


class TriggerStateMachine(object):
    def __init__(self, event, context, cnf):
        self.item = None
        self.pipeline_config_cadence = None
        self.control_file = None
        self.copy_matrix = None
        self.pipeline_meta_path = None
        self.incoming_objects = None
        self.destination_key = None
        self.s3_payload = {}
        self.log = setup_logger()
        self.event = event
        self.context = context
        self.cnf = cnf
        self.s3 = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        self.step_function = boto3.client("stepfunctions")
        self.today = datetime.today()
        self.exec_date = None
        self.step_function_payload = {}
        self.glue_bucket: str = self.cnf.S3_GLUE_BUCKET_NAME

    def get_s3_file_content(self, s3_path: str) -> str:
        try:
            o = urlparse(s3_path)
            bucket = o.netloc
            key = o.path
            boto3.resource("s3")
            obj = self.s3_resource.Object(bucket, key.lstrip("/"))
            file_content = obj.get()["Body"].read().decode("utf-8")
            return file_content
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchKey":
                self.log.info(f"No object found:{s3_path}")
                return "{}"
            else:
                raise

    def s3_upload_dict_to_file(self, dest_bucket: str, dest_prefix: str, content: str):
        self.s3.put_object(Bucket=dest_bucket, Key=dest_prefix, Body=content)

    def get_ingested_s3_object(self, s3_payload) -> dict:
        self.log.info("In get_ingested_s3_object module")
        return {
            "bucket": s3_payload["bucket"]["name"],
            "key": s3_payload["object"]["key"],
            "key_path": os.path.dirname(s3_payload["object"]["key"]),
            "key_name": os.path.basename(s3_payload["object"]["key"]),
            "size": str(
                s3_payload["object"]["size"]
            ),  # Step function mandates this to be string
        }

    def get_objects_in_s3_path(
        self, bucket_name: str, bucket_path: str, ignore_trigger_file: bool = True
    ) -> list:
        self.log.info("In get_objects_in_s3_path module")
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=bucket_path)
        s3_objects = []
        for page in pages:
            try:
                for obj in page["Contents"]:
                    # if ignore_trigger_file and obj["Key"].find(self.control_file) >= 0:
                    #     pass
                    if obj["Key"][len(obj["Key"]) - 1] == "/":
                        pass
                    else:
                        s3_objects.append(obj["Key"])
            except KeyError:
                logging.info(
                    f"No items in s3 bucket {bucket_name} in path {bucket_path}"
                )

        return s3_objects

    def check_s3_key(self, bucket: str = None, key: str = None):
        """Give bucket and key, returns true if key exists in the bucket"""
        self.log.info("In check_s3_key module")
        try:
            self.s3.get_object(
                Bucket=bucket,
                Key=key,
            )
            return True
        except self.s3.exceptions.NoSuchKey:
            return False

    def get_destination_key(self, destination_type: str = "input") -> str:
        self.log.info("In get_destination_key module")
        return (
            f"{destination_type}/{self.cnf.ALL_INGESTION_DIR_NAME}"
            f"/{self.exec_date}"
            f"/{self.s3_payload['key'].split('/')[-1]}"
        )

    def is_file_to_be_omitted(self, file_name: str) -> bool:
        self.log.info("In is_file_to_be_omitted module")
        print(f"is_file_to_be_omitted ==> {file_name}")
        for exclusion_suffix in self.cnf.SUFFIX_FILES_TO_OMIT:
            if file_name.find(exclusion_suffix) >= 0:
                return False
        return True

    def get_destination_matrix(self) -> list:
        self.log.info("In get_destination_matrix module")
        # create list of src_bucket, source_file_path, dest_bucket, dest_file_path
        copy_matrix = []
        self.pipeline_meta_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            self.cnf.PIPELINE_META_DIR,
            f"{os.path.splitext(self.s3_payload['key_name'])[0]}.json",
        )

        self.incoming_objects = self.get_objects_in_s3_path(
            bucket_name=self.s3_payload["bucket"],
            bucket_path=self.s3_payload["key_path"],
        )

        with open(self.pipeline_meta_path) as meta_file:
            expected_files = json.load(meta_file)
            for file in expected_files["registered_incoming_files"]:
                print(f"get_destination_matrix ==>  {file}")
                if self.is_file_to_be_omitted(
                    file_name=self.get_src_file_path(file["prefixes"])
                ):
                    copy_matrix_item = {}
                    copy_matrix_item["table_name"] = file["table_name"]
                    copy_matrix_item["partitioned"] = file["partitioned"]
                    source_file = self.get_src_file_path(file["prefixes"])
                    copy_matrix_item["src_file_path"] = source_file
                    copy_matrix_item["src_bucket"] = self.s3_payload["bucket"]
                    copy_matrix_item["dest_bucket"] = self.s3_payload["bucket"]

                    copy_matrix_item["dest_file_path"] = (
                        f"{self.cnf.LANDING_DB_NAME}/{copy_matrix_item['table_name']}/"
                        f"exec_date={self.exec_date}/{os.path.basename(source_file)}"
                        if copy_matrix_item["partitioned"].lower() == "true"
                        else f"{self.cnf.LANDING_DB_NAME}/{copy_matrix_item['table_name']}/"
                        f"{os.path.basename(source_file)}"
                    )
                    copy_matrix_item["table_name"] = copy_matrix_item["table_name"]
                    copy_matrix.append(copy_matrix_item)

        return copy_matrix

    def get_table_name(
        self, file_type: str, data_file_prefix: str, table_prefix: str
    ) -> (str, None):
        self.log.info("In get_table_name module")
        file_type_prefix = f"{file_type}_"
        if data_file_prefix.find(file_type_prefix) != -1:
            return f"{table_prefix.strip()}{data_file_prefix[len(file_type_prefix):-1]}"
        else:
            return None

    def get_src_file_path(self, data_file_prefix: str) -> (str, None):
        self.log.info("In get_src_file_path module")
        for incoming_file in self.incoming_objects:
            if incoming_file.find(data_file_prefix) >= 0:
                return incoming_file

        raise Exception(f"File {data_file_prefix} not found")

    def get_pipeline_type(self) -> str:
        self.log.info("In get_pipeline_type module")
        try:
            pipeline_type = self.cnf.DATA_PIPELINE[self.s3_payload["key_name"]]["type"]
            return pipeline_type
        except KeyError as e:
            self.log.error(
                f"ERROR: Unexpected data file received : {self.s3_payload['key']}"
            )
            raise e

    def copy_file(
        self, src_bucket: str, src_key: str, dest_bucket: str, dest_key: str
    ) -> str:
        """copies file to the destination within the same bucket"""
        self.log.info("In copy_file module")
        self.s3.copy(
            CopySource={"Bucket": src_bucket, "Key": src_key},
            Bucket=dest_bucket,
            Key=dest_key,
        )
        return dest_key

    def get_glue_runtime_sql_params_payload(self, key: str = "glue_runtime_sql_params"):
        self.log.info("In get_glue_runtime_sql_params_payload module")
        payload = {}
        for k, v in self.cnf.DATA_PIPELINE[self.s3_payload["key_name"]][key].items():
            payload[k] = self.destination_key if k == "param_data_file_key" else v
        payload["param_execution_date"] = self.exec_date
        payload["frequency"], payload["monthly"] = self.get_cadence()
        # payload["frequency"] = self.pipeline_config_cadence
        self.step_function_payload["glue_runtime_sql_params"] = json.dumps(payload)
        self.log.info(f"Parameters for {key} = {payload}")

    def get_step_function_input(self):
        self.log.info("In get_step_function_input module")
        self.step_function_payload.update({"date": self.exec_date})
        self.step_function_payload.update({"pipeline_type": self.get_pipeline_type()})
        # Add other top level step function payloads
        if (
            "step_function_payloads"
            in self.cnf.DATA_PIPELINE[self.s3_payload["key_name"]]
        ):
            for k, v in self.cnf.DATA_PIPELINE[self.s3_payload["key_name"]][
                "step_function_payloads"
            ].items():
                self.step_function_payload.update({k: v})
        # Pipeline Specific payload
        if (
            "glue_runtime_sql_params"
            in self.cnf.DATA_PIPELINE[self.s3_payload["key_name"]]
        ):
            self.get_glue_runtime_sql_params_payload()
        return self.step_function_payload

    def get_ssm_value(self, parameter_name: str) -> str:
        self.log.info("In get_ssm_value module")
        ssm = boto3.client("ssm")
        parameter = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        return parameter["Parameter"]["Value"]

    def get_statemachine_arn(self, parameter_name: str) -> (str, list):
        self.log.info("In get_statemachine_arn module")
        step_name = self.get_ssm_value(parameter_name=parameter_name)

        return f"arn:aws:states:{self.cnf.REGION}:{self.cnf.ACCOUNT}:stateMachine:{step_name}"

    def get_cadence(self) -> (str, str):
        """Get cadence and set monthly/yearly flags"""
        cadence = self.item["cadence"]
        self.step_function_payload["monthly"] = (
            "true" if self.today.date() == self.item["monthly"] else "false"
        )

        self.step_function_payload["yearly"] = (
            "true" if self.today.date() == self.item["yearly"] else "false"
        )

        return cadence, self.step_function_payload["monthly"]

    def get_step_executions(self, step_function_arn: str) -> int:
        """
        Given the stepfunction arn returns the running counts
        """
        sm_execs = boto3.client("stepfunctions").list_executions(
            stateMachineArn=step_function_arn, statusFilter="RUNNING", maxResults=100
        )

        return len(sm_execs["executions"])

    def trigger_statemachine(self) -> [dict]:
        self.log.info("In trigger_statemachine module")
        responses = []
        # fmt: off
        if self.cnf.DATA_PIPELINE[self.s3_payload["key_name"]]["trigger_statemachine"].lower() == "true":  # noqa
            # Check cadence
            for item in self.cnf.DATA_PIPELINE[self.s3_payload["key_name"]]["workflows"]:  # noqa
                # fmt: on
                self.item = item
                self.pipeline_config_cadence = item["cadence"].lower()
                ssm_step_function_name = item["param_store_state_machine_name"]
                step_payload = self.get_step_function_input()
                self.log.info(
                    f"Starting step function "
                    f"{self.get_statemachine_arn(parameter_name=ssm_step_function_name)} "  # noqa
                    f"with payload {step_payload}"
                )
                step_payload["start_dttm"] = datetime.now().strftime("%Y%m%d%H%M%S")
                step_payload["env"] = self.cnf.STAGE
                # step_payload["frequency"] = self.pipeline_config_cadence
                step_payload["frequency"], _ = self.get_cadence()
                print(
                    f"Starting pipeline "
                    f"{self.cnf.DATA_PIPELINE[self.s3_payload['key_name']]['type']} "
                    f"for cadence {step_payload['frequency']}"
                )
                if self.get_step_executions(self.get_statemachine_arn(parameter_name=ssm_step_function_name)) < 1: # noqa
                    response = self.step_function.start_execution(
                        stateMachineArn=self.get_statemachine_arn(
                            parameter_name=ssm_step_function_name
                        ),
                        input=json.dumps(step_payload, indent=4),
                    )
                    responses.append(response)
                else:
                    self.log.info(f"SKIP: Step function {ssm_step_function_name} "
                                  f"already has atlease 1 RUNNING state "
                                  f"and hence not starting a new instance")
        else:
            response = {
                "status": f"State Machine not set to trigger "
                f"for input file {self.s3_payload['key']} "
            }
            responses.append(response)
        return responses

    def get_exec_date_from_key(self) -> str:
        self.log.info("In get_exec_date_from_key module")
        import re

        day = re.search("\\/\d{4}-\d{2}-\d{2}\\/", self.s3_payload["key"])  # noqa
        date_fmt = day.group()[1:-1]
        # This is to validate date format
        date = datetime.strptime(date_fmt, "%Y-%m-%d").date()  # noqa
        return date_fmt

    def delete_objects_from_s3_path(self, bucket_name: str, bucket_prefix: str) -> int:
        self.log.info("In delete_objects_from_s3_path module")
        i = 0
        if len(bucket_prefix.strip()) > 0:
            objects_to_delete = self.get_objects_in_s3_path(
                bucket_name=bucket_name, bucket_path=bucket_prefix
            )
            for i, key in enumerate(objects_to_delete, 1):
                self.log.info(f"Deleting object {i} : {key}...")
                response = self.s3.delete_object(Bucket=bucket_name, Key=key)
                print(response)
        return i

    def delete_all_table_partition(self, copy_matrix):
        """iterate through the tables and call delete_objects_from_s3_path"""
        self.log.info("In delete_all_table_partition module")
        total_counts = 0
        tables_affected = []
        for item in copy_matrix:
            bucket_name = item["dest_bucket"]
            delete_path = os.path.dirname(item["dest_file_path"])
            if len(delete_path.strip()) > 0:
                del_count = self.delete_objects_from_s3_path(
                    bucket_name=bucket_name, bucket_prefix=delete_path
                )
                total_counts += del_count
                tables_affected.append(item["table_name"])
                self.log.info(
                    f" Total S3 objects removed for table {item['table_name']}"
                    f" in partition {self.exec_date} : {del_count} "
                )

        self.log.info(
            f"\n Total S3 objects removed for tables {tables_affected} "
            f"\n in partition {self.exec_date} : {total_counts} \n"
        )

    def copy_table_files(self):
        self.log.info("In copy_table_files module")
        i = 0
        for i, items in enumerate(self.copy_matrix, 1):  # noqa
            self.copy_file(
                src_bucket=items["src_bucket"],
                src_key=items["src_file_path"],
                dest_bucket=items["dest_bucket"],
                dest_key=items["dest_file_path"],
            )
            self.log.info(
                f"Copied s3://{items['src_bucket']}/{items['src_file_path']}"
                f" ==> s3://{items['dest_bucket']}/{items['dest_file_path']}"
            )
        self.log.info(f" In all {i} files copied")

    def execute(self):
        """Driver module"""
        import os

        self.log.info("Started Workflow Trigger")
        for record in self.event["Records"]:
            # fmt: off
            if record["eventSource"] == "aws:s3" and record["awsRegion"] == self.cnf.REGION:  # noqa
                # fmt: on
                self.s3_payload = self.get_ingested_s3_object(record["s3"])
                self.control_file = self.s3_payload["key_name"]
                # Code to restrict execution during folder creation
                if (
                    os.path.basename(self.s3_payload["key"]).find(".") < 0
                ):  # noqa # looking for file with extension
                    self.log.info(
                        f"Possible folder creation: "
                        f"Exiting as no file name in "
                        f"path {self.s3_payload['key']}"
                    )
                    return
                self.exec_date = self.get_exec_date_from_key()
                self.destination_key = self.get_destination_key()
                self.copy_matrix = self.get_destination_matrix()
                # Clean up prior files loaded to the same partition if any
                self.delete_all_table_partition(self.copy_matrix)
                # Copy file
                self.copy_table_files()
                step_status = self.trigger_statemachine()
                self.log.info(step_status)
                self.log.info("Completed TriggerStageMachine")
                return json.dumps(step_status, default=str)


if __name__ == "__main__":
    """Run Lambda Function locally"""
    from ast import literal_eval as le
    from pprint import pprint as pp

    # from data.s3_trigger_payload import PAYLOAD_EF_WF1 as PAYLOAD
    PAYLOAD = {}
    event_package = le(PAYLOAD)
    pp(handler(event_package, {}))
