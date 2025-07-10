#!/usr/bin/python3
"""
    The Lambda function is responsible for creating
    {pipeline}.done file in the incoming folder
"""
import json
import os
import boto3
import config
from common.log_utils import setup_logger
import datetime
from io import BytesIO


def handler(event, context):
    """
    Lambda function is responsible for the following,
        1. Verify if the .done file is present in the incoming folder,
            if so, no action taken
        2. Verify if all incoming files are present against the pipeline metadata.
            If more or less files are present,done file is not created.
        3. If all checks are passed, it creates 0 byte .done file
           , e.g. state_emission.done
    TO-DO:
        1. If file not received by the end of the day?
    """
    print(f"Done Lambda payload = {event}")
    ex = CreateDoneFile(event=event, context=context, cnf=config)
    return ex.execute()


class CreateDoneFile(object):
    def __init__(self, event, context, cnf):
        self.event = event
        self.context = context
        self.pipeline_meta_path = None
        self.log = setup_logger()
        self.cnf = cnf
        self.exec_type = None
        try:
            self.today = datetime.datetime.strptime(
                event["payload"]["run_exec_date"], "%Y-%m-%d"
            ).date()
        except KeyError:
            self.today = datetime.datetime.today().date()
            pass
        try:
            self.create_done_prop = event["payload"]["create_done"]
        except KeyError:
            self.create_done_prop = None
            pass

        self.s3 = boto3.client("s3")

    def create_done_file(self, bucket_path, done_file_name) -> bool:
        self.log.info("In create_done_file module.")
        self.log.info(
            f"All required files are present. Creating control file {done_file_name}"
        )
        fileobj = BytesIO()
        self.s3.upload_fileobj(
            fileobj, self.cnf.S3_LANDING_BUCKET_NAME, bucket_path + done_file_name
        )
        self.log.info(
            f"Successfully created {self.cnf.S3_LANDING_BUCKET_NAME}/"
            f"{bucket_path}{done_file_name} "
        )
        return True

    # move the bucket out and use self. Data types
    def match_incoming_file(
        self, objects_in_s3_path, expected_data_files_prefixes, done_file_name
    ) -> bool:
        self.log.info(
            "In match_incoming_file module. \n"
            "Start the match process to see if all files are present "
            "and only required files are present.One of each type."
        )

        # check if .done file exists. if so, return without creating a new file
        for file in objects_in_s3_path:
            if file == done_file_name and self.exec_type == "self_pipeline":
                self.log.info(
                    f"{file} is already present. "
                    f"Manually remove the .done file for rerun"
                )
                return False

        # match the incoming files with the data_file_prefixes.
        # There should be exact 1 file for each data file prefixes
        incoming_file_match_count = 0
        data_file_prefixes_count = len(expected_data_files_prefixes)

        for incoming_file in objects_in_s3_path:
            for data_file_prefix in expected_data_files_prefixes:
                if incoming_file.find(data_file_prefix) >= 0:
                    incoming_file_match_count = incoming_file_match_count + 1
                    break

        self.log.info(
            f"{incoming_file_match_count} incoming files matched with "
            f"{data_file_prefixes_count} data file prefixes."
        )

        if incoming_file_match_count != data_file_prefixes_count:
            self.log.info(
                f"{done_file_name} not created: Number and/or name of incoming file do not match with data prefixes."
            )
            return False

        return True

    def get_expected_data_files_prefixes(self, pipeline) -> list:
        self.log.info("In get_expected_data_files_prefixes module.")
        self.pipeline_meta_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            self.cnf.PIPELINE_META_DIR,
            f"{pipeline}.json",
        )

        self.log.info(f"pipeline meta file to be read is {self.pipeline_meta_path}.")
        data_file_prefixes = []
        with open(self.pipeline_meta_path) as meta_file:
            expected_files = json.load(meta_file)["registered_incoming_files"]
            for value in expected_files:
                data_file_prefixes.append(value["prefixes"])
        self.log.info(
            f"List of all data file prefixes for pipeline {pipeline} is: "
            f"{','.join(data_file_prefixes)}."
        )
        return data_file_prefixes

    def get_objects_in_s3_path(self, bucket_path) -> list:
        self.log.info("In get_objects_in_s3_path module.")
        s3_list_objects_response = self.s3.list_objects(
            Bucket=self.cnf.S3_LANDING_BUCKET_NAME, Prefix=bucket_path
        )
        if (
            "Contents" not in s3_list_objects_response
        ):  # folder (e.g. 2023-03-03) not present.
            self.log.info(
                f"Incoming folder {bucket_path} not present in bucket "
                f"{self.cnf.S3_LANDING_BUCKET_NAME}."
            )
            return []

        s3_objects = []
        # get the files from the latest exec date folder and exclude the
        # folder name from the list
        s3_objects = [
            item["Key"].replace(bucket_path, "").strip()
            for item in s3_list_objects_response["Contents"]
            if item["Key"] != bucket_path
        ]
        return s3_objects

    def check_cadence(self, pipeline_value: dict) -> bool:
        for schedule in pipeline_value["workflows"]:
            if schedule["cadence"] == "daily":
                return True
        return False

    def create_dependent_pipeline_completed(
        self, bucket_path: str, pipeline_props: dict
    ):
        for done_file in pipeline_props["create_wf_dependency_done_file"]:
            self.create_done_file(bucket_path, done_file)

        self.create_done_file(bucket_path, f"{pipeline_props['type']}.completed")

    def create_done_file_logic(self, pipeline: str, pipeline_props: dict) -> dict:
        """
        create_done_file
            True  ==> creates .done file for the current pipeline
            False ==> creates .done file(s) specified
                      in create_wf_dependency_done_file config

        """

        self.log.info("\n")
        # fmt: off
        bucket_path = pipeline_props['incoming_path'] + '/' + self.today.strftime('%Y-%m-%d') + '/'  # noqa
        # fmt: on
        self.log.info(
            f"########      Verify for pipeline {pipeline_props['type']} for "
            f"date {self.today}     ########"
        )
        self.log.info(
            f"Get the list of objects present in {self.cnf.S3_LANDING_BUCKET_NAME} "
            f"and folder {bucket_path}."
        )
        objects_in_s3_path = self.get_objects_in_s3_path(bucket_path)

        self.log.info(
            f"Get the list of expected data file prefixes for pipeline "
            f"{pipeline.split('.')[0]}."
        )
        expected_data_files_prefixes = self.get_expected_data_files_prefixes(
            pipeline.split(".")[0]
        )

        done_file_name = (
            pipeline  # done file name is same as pipeline
        )

        if self.match_incoming_file(
            objects_in_s3_path, expected_data_files_prefixes, done_file_name
        ):
            # TO CHECK CADENCE
            if self.check_cadence(pipeline_props):
                self.log.info(
                    f"Creating `{done_file_name}` file: "
                    f"All conditions met & cadence met"
                )
                return {
                    "status": True,
                    "bucket": bucket_path,
                    "done_file": done_file_name,
                }
            else:
                self.log.info(
                    f"Not creating `{done_file_name}` file: All conditions met, "
                    f"but cadence not met"
                )

        return {"status": False, "bucket": "", "done_file": ""}

    def get_pipeline_info(self, target_pipeline_name: str) -> (str, dict):
        for pipeline, pipeline_name in self.cnf.DATA_PIPELINE.items():
            if pipeline_name["type"] == target_pipeline_name:
                return pipeline, pipeline_name
        raise Exception(f"{target_pipeline_name} not found in configuration ")

    def execute(self):
        """Driver module"""
        self.log.info("Started Create Done File Steps")
        if self.create_done_prop is not None:
            self.exec_type = "dependency_pipeline"
            self.today = datetime.datetime.strptime(
                self.create_done_prop["exec_date"], "%Y-%m-%d"
            ).date()
            pipeline, pipeline_props = self.get_pipeline_info(
                target_pipeline_name=self.event["payload"]["create_done"][
                    "pipeline_name"
                ]
            )
            response = self.create_done_file_logic(
                pipeline=pipeline, pipeline_props=pipeline_props
            )
            if response["status"]:
                self.create_dependent_pipeline_completed(
                    bucket_path=response["bucket"], pipeline_props=pipeline_props
                )
        else:
            self.exec_type = "self_pipeline"
            for pipeline, pipeline_props in self.cnf.DATA_PIPELINE.items():
                response = self.create_done_file_logic(
                    pipeline=pipeline, pipeline_props=pipeline_props
                )
                if response["status"]:
                    self.create_done_file(response["bucket"], response["done_file"])


if __name__ == "__main__":
    """Run Lambda Function locally"""

    PAYLOAD = {}

    handler(event=PAYLOAD, context={})
