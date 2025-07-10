"""Iam helpers for data resource permissioning"""
import os
from dataclasses import dataclass, field
from typing import List, Optional

from aws_cdk import aws_iam as iam, aws_s3 as s3, Arn

import config as cf
from pkg import s3_helpers

DEFAULT_PRODUCER_ACTIONS = [
    "s3:AbortMultipartUpload",
    "s3:ListMultipartUploadParts",
    "s3:GetBucketLocation",
    "s3:GetObject",
    "s3:ListBucket",
    "s3:ListBucketMultipartUploads",
    "s3:PutObject",
    "s3:PutObjectAcl",
]

DEFAULT_CONSUMER_ACTIONS = [
    "s3:GetBucket*",
    "s3:GetObject*",
    "s3:ListBucket",
]


@dataclass
class S3TableLocation:
    """Class responsible for generating s3 and glue access policy statements"""

    bucket_type: str
    database: str
    tables: List[str] = field(
        default_factory=lambda: ["*"]
    )  # default to all tables if unspecified

    def __post_init__(self):
        """Validate bucket_type"""
        s3_helpers.validate_bucket_type(self.bucket_type)

    def get_s3_policy(self, access_level: str) -> iam.PolicyStatement:
        """Generates S3 read or write policy statement"""
        read_actions = ["s3:GetObject*", "s3:GetBucket*", "s3:List*", "s3:Head*"]
        write_actions = [
            "s3:PutObject*",
            "s3:Abort*",
            "s3:DeleteObject*",
        ] + read_actions
        bucket_arn = s3_helpers.get_bucket_arn(self.bucket_type)
        db_folder = f"{bucket_arn}/{self.database}_$folder$"
        resources = [bucket_arn, db_folder]

        db_s3_arn = os.path.join(bucket_arn, self.database)
        if "*" in self.tables:
            resources.append(os.path.join(db_s3_arn, "*"))
        else:
            for table in self.tables:
                resources.append(os.path.join(db_s3_arn, table, "*"))
                resources.append(os.path.join(db_s3_arn, f"{table}_$folder$"))
        actions = write_actions if access_level == "write" else read_actions
        return iam.PolicyStatement(actions=actions, resources=resources)

    def get_glue_policy(self, access_level: str) -> iam.PolicyStatement:
        """Generates Glue database and table policy statement"""
        read_actions = ["glue:Get*", "glue:BatchGet*"]
        write_actions = [
            "glue:CreateTable",
            "glue:CreatePartition",
            "glue:UpdatePartition",
            "glue:UpdateTable",
            "glue:DeleteTable",
            "glue:DeletePartition",
            "glue:BatchCreatePartition",
        ] + read_actions
        base_arn = f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}"
        table_resources = (
            [f"{base_arn}:table/{self.database}/*"]
            if "*" in self.tables
            else [f"{base_arn}:table/{self.database}/{table}*" for table in self.tables]
        )
        resources = [
            f"{base_arn}:catalog",
            f"{base_arn}:database/{self.database}",
        ] + table_resources
        actions = write_actions if access_level == "write" else read_actions
        return iam.PolicyStatement(actions=actions, resources=resources)


def create_athena_query_policy(workgroup: str = "primary") -> iam.PolicyStatement:
    """Creates iam.PolicyStatement for athena query access"""
    return iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        actions=[
            "athena:StartQueryExecution",
            "athena:StopQueryExecution",
            "athena:GetDataCatalog",
            "athena:GetQueryResults",
            "athena:GetQueryExecution",
        ],
        resources=[
            f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:catalog",
            f"arn:aws:athena:{cf.REGION}:{cf.ACCOUNT}:datacatalog/AwsDataCatalog",
            f"arn:aws:athena:{cf.REGION}:{cf.ACCOUNT}:workgroup/{workgroup}",
        ],
    )


# TODO: Migrate to create_s3_policy and deprecate this
def create_s3_producer_policy(
    principal_arn: Optional[str], bucket: s3.Bucket, prefix: str = "*"
) -> iam.PolicyStatement:
    """Create S3 producer policystatement for role or user principals"""
    account = Arn.parse(principal_arn).account
    return iam.PolicyStatement(
        actions=[
            "s3:AbortMultipartUpload",
            "s3:ListMultipartUploadParts",
            "s3:GetBucketLocation",
            "s3:GetObject",
            "s3:ListBucket",
            "s3:ListBucketMultipartUploads",
            "s3:PutObject",
            "s3:PutObjectAcl",
        ],
        resources=[bucket.bucket_arn, f"{bucket.bucket_arn}/{prefix}"],
        principals=[iam.AccountPrincipal(account)],
        conditions={"ArnEquals": {"aws:PrincipalArn": principal_arn}},
    )


# def parse_consumer_to_policy(
#     bucket: s3.Bucket, actions: List[str], consumer: ConsumerAccount
# ) -> List[Dict]:
#     """
#     This is probably not ideal. The goal here is to bridge between "consumer" config & "create_s3_policy" method. # noqa
#
#     :param bucket: bucket reference from storage stack
#     :param actions: actions attached to the consumers after composition
#     :param consumer: config of a consumer AWS account
#     :return: list of kwargs for create_s3_policy to use
#     """
#     result = []
#     for database, table_to_roles in consumer.db_to_table_to_roles.items():
#         for table, roles in table_to_roles.items():
#             result.append(
#                 {
#                     "bucket": bucket,
#                     "actions": actions,
#                     "principal_arns": roles,
#                     "prefix": f"{database}/{table}/*",
#                 }
#             )
#
#     return result


def create_s3_policy(
    principal_arns: List[str], bucket: s3.Bucket, actions: List[str], prefix: str = "*"
) -> iam.PolicyStatement:
    """
    Create cross-account s3 bucket policy for given principal_arns under an AWS account.
    Note: All principal arns has to be from the same account for lazy evaluation condition.

    We leave the producer/consumer definition on action variable names.
    The long term vision for this is action composition for easier security review.

    For example, we have a producer with 2 delivery mechanisms.
    The corresponding AWS service for delivery are Lambda/DynamoDBExport.

    We want to have different actions as followed (Made up):

    LAMBDA_PRODUCER_ACTIONS = [
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts",
        "s3:GetBucketLocation",
        "s3:ListBucket",
        "s3:PutObject",
    ]

    DDB_EXPORT_PRODUCER_ACTIONS = [
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:PutObject",
        "s3:PutObjectAcl",
    ]

    Final actions after composition for "S3 landing bucket" will be:
    PRODUCER_ACTIONS = [
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts",
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:PutObject",
        "s3:PutObjectAcl",
    ]

    Upon security review, we will provide PRODUCER_ACTIONS with granular breakdown to answer # noqa
    "s3:PutObjectAcl" is needed for DDB_EXPORT, but not for LAMBDA.

    :param principal_arns: List of IAM policy principal_arns under an AWS account
    :param bucket: S3 bucket arn
    :param actions: List of IAM policy actions
    :param prefix: str prefix for S3 to whitelist on
    :return result: iam policy statement
    """
    if not principal_arns:
        raise ValueError("principal_arns should be non-empty")

    # Validate all arns are from same account
    accounts = [Arn.parse(principal_arn).account for principal_arn in principal_arns]
    under_same_account = len(set(accounts)) == 1

    if not under_same_account:
        raise ValueError("principal_arns should be under same account")

    return iam.PolicyStatement(
        actions=actions,
        resources=[bucket.bucket_arn, f"{bucket.bucket_arn}/{prefix}"],
        principals=[iam.AccountPrincipal(accounts[0])],
        conditions={"ForAnyValue:ArnEquals": {"aws:PrincipalArn": principal_arns}},
    )
