"""Stack to manage raw data processing and access"""
# pylint: disable=unused-variable, too-many-locals
from aws_cdk import (
    aws_cloudtrail as trail,
    aws_iam as iam,
    aws_s3 as s3,
    RemovalPolicy,
    Duration,
)
from aws_cdk import Stack
from constructs import Construct

import config as cf


class LakeStorageStack(Stack):
    """Stack to provision S3 storage layer"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.glue_role = iam.Role(
            self,
            id="glue-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name="APG-LakeStorage-AWSGlueServiceRole",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        self.log_bucket = s3.Bucket(
            self,
            id="logs_bucket",
            bucket_name=cf.S3_LOG_BUCKET,
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            removal_policy=RemovalPolicy.RETAIN,
            enforce_ssl=True,
            server_access_logs_prefix="access_logs/",
        )
        self.log_bucket.grant_read_write(
            iam.ServicePrincipal("cloudtrail.amazonaws.com")
        )
        self.trail = bucket_cloudtrail = trail.Trail(  # noqa
            self,
            id="s3-cloudtrail",
            bucket=self.log_bucket,
            send_to_cloud_watch_logs=True,
        )
        self.trail.log_all_s3_data_events()
        self.landing_bucket = self.create_and_register_bucket(cf.S3_LANDING_BUCKET)
        self.processed_bucket = self.create_and_register_bucket(cf.S3_PROCESSED_BUCKET)

        athena_bucket = self.create_and_register_bucket(cf.S3_ATHENA_BUCKET)
        glue_assets_bucket = self.create_and_register_bucket(cf.S3_GLUE_ASSETS_BUCKET)
        glue_assets_bucket = self.create_and_register_bucket(cf.S3_AUDIT_BUCKET)
        self.trail.add_s3_event_selector(
            s3_selector=[
                trail.S3EventSelector(bucket=self.landing_bucket),
                trail.S3EventSelector(bucket=self.processed_bucket),
                trail.S3EventSelector(bucket=athena_bucket),
                trail.S3EventSelector(bucket=glue_assets_bucket),
            ]
        )

    def create_and_register_bucket(self, bucket_name: str) -> s3.Bucket:
        """Creates and registers bucket to Lakeformation"""
        bucket = s3.Bucket(
            self,
            id=bucket_name,
            bucket_name=bucket_name,
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            versioned=False,
            enforce_ssl=True,
            server_access_logs_prefix="access_logs/",
            lifecycle_rules=[
                s3.LifecycleRule(
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(0),
                        )
                    ],
                    noncurrent_version_transitions=[
                        s3.NoncurrentVersionTransition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(0),
                        )
                    ],
                )
            ],
        )
        bucket.grant_read_write(self.glue_role)

        return bucket
