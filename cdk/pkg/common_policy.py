from aws_cdk import aws_iam as iam, aws_ssm as ssm,Fn
from aws_cdk.aws_iam import Policy, Role
from constructs import Construct
import config as cf
from pkg import iam_helpers

from pkg.glue_helpers import setup_glue_role


def create_standard_glue_job_role(scope: Construct, iam_role_name: str) -> Role:
    standard_glue_job_role = setup_glue_role(
        scope,
        role_name=iam_role_name,
        source_locations=[
            iam_helpers.S3TableLocation("landing", cf.LANDING_DB_NAME),
            iam_helpers.S3TableLocation("processed", cf.PROCESSED_DB_NAME),
            iam_helpers.S3TableLocation("audit", cf.AUDIT_DB_NAME),
        ],
        target_locations=[
            iam_helpers.S3TableLocation("processed", cf.PROCESSED_DB_NAME),
            iam_helpers.S3TableLocation("audit", cf.AUDIT_DB_NAME),
        ],
        direct_catalog_access=True,
    )

    attach_common_polices_to_role(scope=scope, iam_role=standard_glue_job_role)
    return standard_glue_job_role


def attach_common_polices_to_role(scope: Construct, iam_role: Role) -> Role:
    iam_role.attach_inline_policy(
        athena_exec_perm(scope, "athena-query-exec-permissions")
    )
    iam_role.attach_inline_policy(glue_perm(scope, "glue-catalog-permissions"))
    iam_role.attach_inline_policy(lake_buckets_perm(scope, "lake-buckets-permissions"))
    iam_role.attach_inline_policy(
        athena_query_results_bucket_perm(
            scope, "athena-query-results-bucket-permissions"
        )
    )
    iam_role.attach_inline_policy(
        glue_job_kms_perm(scope, "glue-catalog-kms-permissions")
    )


def athena_exec_perm(scope: Construct, id: str) -> Policy:
    return Policy(
        scope,
        id=id,
        policy_name=id,
        document=iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "athena:StartQueryExecution",
                        "athena:StopQueryExecution",
                        "athena:GetQueryExecution",
                        "athena:GetQueryResults",
                        "athena:GetWorkGroup",
                    ],
                    resources=[f"arn:aws:athena:{cf.REGION}:{cf.ACCOUNT}:workgroup/*"],
                )
            ]
        ),
    )


def glue_job_kms_perm(scope: Construct, id: str) -> Policy:
    def get_key_arn_list(ssm_param_stack_op_name_list: list) -> list:
        key_arn_list = []
        for ssm_param_stack_op_name in ssm_param_stack_op_name_list:
            ssm_param_name = Fn.import_value(ssm_param_stack_op_name)
            key_string_parameter = ssm.StringParameter.from_string_parameter_attributes(
                scope,
                f"{ssm_param_stack_op_name}-id",
                parameter_name=ssm_param_name,
                simple_name=True,
            )
            key_arn_list.append(key_string_parameter.string_value)
        return key_arn_list

    stack_op_name_list = [cf.CATALOG_KEY_ARN, cf.GLUE_SECURITY_CONFIG_CW_KEY_ARN]

    glue_kms_arns = get_key_arn_list(ssm_param_stack_op_name_list=stack_op_name_list)
    return Policy(
        scope,
        id=id,
        policy_name=id,
        document=iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "kms:ListKeys",
                        "kms:Decrypt",
                        "kms:Encrypt",
                        "kms:GenerateDataKey",
                    ],
                    resources=glue_kms_arns,
                )
            ]
        ),
    )


def glue_perm(scope: Construct, id: str) -> Policy:
    return Policy(
        scope,
        id=id,
        policy_name=id,
        document=iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "glue:GetDatabases",
                        "glue:GetTable",
                        "glue:GetPartition",
                        "glue:CreatePartition",
                        "glue:DeletePartition",
                        "glue:UpdatePartition",
                        "glue:CreateTable",
                        "glue:GetTables",
                        "glue:GetPartitions",
                        "glue:UpdateTable",
                        "glue:BatchGetPartition",
                        "glue:DeleteTable",
                        "glue:BatchCreatePartition",
                        "glue:CreatePartitionIndex",
                        "glue:BatchUpdatePartition",
                        "glue:BatchDeletePartition",
                        "glue:GetPartitionIndexes",
                        "glue:GetJobRuns",
                        "glue:GetJobRun",
                    ],
                    resources=[
                        f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:database/*",
                        f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:catalog",
                        f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:table/*/*",
                        f"arn:aws:glue:{cf.REGION}:{cf.ACCOUNT}:job/*",
                    ],
                )
            ]
        ),
    )


def lake_buckets_perm(scope: Construct, id: str) -> Policy:
    return Policy(
        scope,
        id=id,
        policy_name=id,
        document=iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:ListBucketMultipartUploads",
                        "s3:ListBucket",
                        "s3:ListMultipartUploadParts",
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:AbortMultipartUpload",
                        "s3:DeleteObject",
                        "s3:GetBucketLocation",
                    ],
                    resources=[
                        f"arn:aws:s3:::{cf.S3_LANDING_BUCKET}",
                        f"arn:aws:s3:::{cf.S3_LANDING_BUCKET}/*",
                        f"arn:aws:s3:::{cf.S3_PROCESSED_BUCKET}",
                        f"arn:aws:s3:::{cf.S3_PROCESSED_BUCKET}/*",
                        f"arn:aws:s3:::{cf.S3_GLUE_ASSETS_BUCKET}",
                        f"arn:aws:s3:::{cf.S3_GLUE_ASSETS_BUCKET}/*",
                        f"arn:aws:s3:::{cf.S3_AUDIT_BUCKET}",
                        f"arn:aws:s3:::{cf.S3_AUDIT_BUCKET}/*",
                    ],
                )
            ]
        ),
    )


def athena_query_results_bucket_perm(scope: Construct, id: str) -> Policy:
    return Policy(
        scope,
        id=id,
        policy_name=id,
        document=iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:ListBucketMultipartUploads",
                        "s3:CreateBucket",
                        "s3:ListBucket",
                        "s3:ListMultipartUploadParts",
                        "s3:*Object",
                        "s3:AbortMultipartUpload",
                        "s3:GetBucketLocation",
                    ],
                    resources=["arn:aws:s3:::aws-athena-query-results-*"],
                )
            ]
        ),
    )
