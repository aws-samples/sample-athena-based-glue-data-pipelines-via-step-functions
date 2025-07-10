"""Stack to manage raw data processing and access"""

from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_kms as kms,
    CfnOutput,
    aws_ssm as ssm,
    aws_glue_alpha as _aws_glue_alpha,
    aws_iam as iam,
)
from constructs import Construct
import config as cf


class LakeStack(Stack):
    """Stack to create Glue database"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Glue Databases
        glue.CfnDatabase(  # noqa
            self,
            cf.LANDING_DB_NAME,
            catalog_id=cf.ACCOUNT,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=cf.LANDING_DB_NAME,
                description="Glue database for data in landing zone",
                location_uri=f"s3://{cf.S3_LANDING_BUCKET}/{cf.LANDING_DB_NAME}",
            ),
        )

        glue.CfnDatabase(  # noqa
            self,
            cf.PROCESSED_DB_NAME,
            catalog_id=cf.ACCOUNT,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=cf.PROCESSED_DB_NAME,
                description="Glue database for data in processed zone",
                location_uri=f"s3://{cf.S3_PROCESSED_BUCKET}/{cf.PROCESSED_DB_NAME}",
            ),
        )
        glue.CfnDatabase(  # noqa
            self,
            cf.AUDIT_DB_NAME,
            catalog_id=cf.ACCOUNT,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=cf.AUDIT_DB_NAME,
                description="Glue database for data in Audit",
                location_uri=f"s3://{cf.S3_AUDIT_BUCKET}/{cf.AUDIT_DB_NAME}",
            ),
        )

        catalog_encryption_key = kms.Key(
            self,
            "glue-catalog-db-Key",
            alias=cf.GLUE_CATALOG_KEY_ALIAS,
            enable_key_rotation=True,
        )

        catalog_key_arn_ssm_parameter = ssm.StringParameter(
            self,
            "catalogKeyParameterArn",
            parameter_name="catalogKeyParameterArn",
            string_value=catalog_encryption_key.key_arn,
        )

        glue.CfnDataCatalogEncryptionSettings(
            self,
            "DataCatalogEncryptionSettings",
            catalog_id=cf.ACCOUNT,
            data_catalog_encryption_settings=glue.CfnDataCatalogEncryptionSettings.DataCatalogEncryptionSettingsProperty(  # noqa
                connection_password_encryption=glue.CfnDataCatalogEncryptionSettings.ConnectionPasswordEncryptionProperty(  # noqa
                    kms_key_id=catalog_encryption_key.key_id,
                    return_connection_password_encrypted=True,
                ),
                encryption_at_rest=glue.CfnDataCatalogEncryptionSettings.EncryptionAtRestProperty(  # noqa
                    catalog_encryption_mode="SSE-KMS",
                    sse_aws_kms_key_id=catalog_encryption_key.key_id,
                ),
            ),
        )

        encryption_key = kms.Key(
            self,
            id="glue-stacks-Key",
            alias=cf.GLUE_CATALOG_CLOUDWATCH_KEY,
            enable_key_rotation=True,
            policy=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        principals=[
                            iam.ServicePrincipal("logs.us-east-1.amazonaws.com")
                        ],
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "kms:Encrypt*",
                            "kms:Decrypt*",
                            "kms:ReEncrypt*",
                            "kms:GenerateDataKey*",
                            "kms:Describe*",
                        ],
                        resources=["*"],
                    ),
                    iam.PolicyStatement(
                        principals=[iam.AccountPrincipal(cf.ACCOUNT)],
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "kms:*",
                        ],
                        resources=["*"],
                    ),
                ]
            ),
        )

        glue_security_cw_kms_arn_ssm_parameter = ssm.StringParameter(
            self,
            "glue-security-config-cw-arn",
            parameter_name="/glue/security-config/cloudwatch/kms-arn",
            string_value=encryption_key.key_arn,
        )

        security_configuration = _aws_glue_alpha.SecurityConfiguration(
            self,
            "XTO-GHG-Glue-SecurityConfiguration",
            cloud_watch_encryption=_aws_glue_alpha.CloudWatchEncryption(
                mode=_aws_glue_alpha.CloudWatchEncryptionMode.KMS,
                kms_key=encryption_key,
            ),
            job_bookmarks_encryption=_aws_glue_alpha.JobBookmarksEncryption(
                mode=_aws_glue_alpha.JobBookmarksEncryptionMode.CLIENT_SIDE_KMS,
                kms_key=encryption_key,
            ),
            s3_encryption=_aws_glue_alpha.S3Encryption(
                mode=_aws_glue_alpha.S3EncryptionMode.KMS, kms_key=encryption_key
            ),
        )

        glue_security_configuration_ssm_parameter = ssm.StringParameter(
            self,
            "glue-security-config-id",
            parameter_name="/glue/security-config/phx23",
            string_value=security_configuration.security_configuration_name,
        )

        CfnOutput(
            self,
            cf.GLUE_CATALOG_KEY_ARN,
            value=catalog_encryption_key.key_arn,
            export_name=cf.GLUE_CATALOG_KEY_ARN,
        )

        CfnOutput(
            self,
            cf.GLUE_JOBS_KEY_ARN,
            value=encryption_key.key_arn,
            export_name=cf.GLUE_JOBS_KEY_ARN,
        )
        CfnOutput(
            self,
            cf.CATALOG_KEY_ARN,
            value=catalog_key_arn_ssm_parameter.parameter_name,
            export_name=cf.CATALOG_KEY_ARN,
        )

        CfnOutput(
            self,
            cf.GLUE_JOBS_SECURITY_CONFIG_NAME,
            value=security_configuration.security_configuration_name,
            export_name=cf.GLUE_JOBS_SECURITY_CONFIG_NAME,
        )

        CfnOutput(
            self,
            cf.GLUE_SECURITY_CONFIG_CW_KEY_ARN,
            value=glue_security_cw_kms_arn_ssm_parameter.parameter_name,
            export_name=cf.GLUE_SECURITY_CONFIG_CW_KEY_ARN,
        )
