"""Constants file for resource naming and env values"""
import os

###################################
# CDK Environment Setup
###################################

ACCOUNT = os.environ["CDK_DEFAULT_ACCOUNT"]
REGION = os.environ["CDK_DEFAULT_REGION"]
DEPLOYMENT_STAGE = os.environ.get("DEPLOYMENT_STAGE", "dev").lower()

CDK_ENV = {"account": ACCOUNT, "region": REGION}

###################################
# Setting up Constants
###################################
# S3
S3_ATHENA_QUERY_FILE_NAME = "exec_athena_query.py"
S3_LANDING_BUCKET = "apg-landing-{account}-{stage}".format(account=ACCOUNT, stage=DEPLOYMENT_STAGE)
S3_PROCESSED_BUCKET = "apg-processed-{account}-{stage}".format(account=ACCOUNT, stage=DEPLOYMENT_STAGE)
S3_AUDIT_BUCKET = "apg-audit-{account}-{stage}".format(account=ACCOUNT, stage=DEPLOYMENT_STAGE)
S3_ATHENA_BUCKET = "aws-athena-query-results-{account}-{region}".format(account=ACCOUNT, region=REGION)
S3_LOG_BUCKET = "apg-cloudtrail-logs-{account}-{stage}".format(account=ACCOUNT, stage=DEPLOYMENT_STAGE)
S3_GLUE_ASSETS_BUCKET = "apg-glue-assets-{account}-{stage}".format(account=ACCOUNT, stage=DEPLOYMENT_STAGE)

# s3 glue asset data validation json file prefix
S3_GLUE_ASSETS_STAGE = "stage"
S3_GLUE_ASSET_FILES = [S3_ATHENA_QUERY_FILE_NAME]

# S3 Landing Incoming path

S3_LANDING_INCOMING_PATH = "incoming"

# ATHENA DB
LANDING_DB_NAME = f"landing_db_{DEPLOYMENT_STAGE}"
PROCESSED_DB_NAME = f"processed_db_{DEPLOYMENT_STAGE}"
AUDIT_DB_NAME = f"audit_db_{DEPLOYMENT_STAGE}"

# AUDIT CONFIGURATION #
# Table
AUDIT_TABLE = "audit"

AUDIT_META = {"param_audit_db": AUDIT_DB_NAME, "param_audit_table": AUDIT_TABLE}
#######################
# Lambda Layer
WRANGLER_ASSET = "awswrangler-layer-3.2.0-py3.9.zip"
WRANGLER_ASSET_VERSION = "3.2.0"
AUDIT_CONFIG_GEN_LAMBDA_NAME = "audit-config-generator-lambda"
DONE_LAMBDA_NAME = "apg-create-done-file-lambda"

PIPELINE_NAME = {
    "usghgemission_daily": "USGHGEFCalculationDaily",
    "usghgemission_monthly": "USGHGEFCalculationMonthly",
}

###################################
# Setting up repo paths
###################################
PATH_CDK = os.path.dirname(os.path.abspath(__file__))
PATH_ROOT = os.path.dirname(PATH_CDK)
PATH_SRC = os.path.join(PATH_ROOT, "src")
PATH_SQL = os.path.join(PATH_ROOT, "sql")

# KMS Keys
GLUE_CATALOG_KEY_ALIAS = "glue/catalog"
GLUE_CATALOG_CLOUDWATCH_KEY = "glue/security-config/cloudwatch"
CATALOG_KEY_ARN = "catalogKMSKeyArn"
GLUE_SECURITY_CONFIG_CW_KEY_ARN = "glueSecurityConfigCWKeyArn"
GLUE_CATALOG_KEY_ARN = "glueCatalogKeyArn"
GLUE_JOBS_SECURITY_CONFIG_NAME = "gluejobsSecurityConfigName"
GLUE_JOBS_KEY_ARN = "gluejobsKeyArn"

 
