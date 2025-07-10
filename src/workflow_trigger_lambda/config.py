"""   Set config variables here   """
import os
import datetime

TODAY = datetime.date.today()

STAGE = os.environ.get("STAGE", "dev")
REGION = os.environ.get("REGION", "us-east-1")
ACCOUNT = os.environ.get("ACCOUNT", "123456789012")

LANDING_DB_NAME = f"landing_db_{STAGE.lower()}"
PROCESSED_DB_NAME = f"processed_db_{STAGE.lower()}"
AUDIT_DB_NAME = f"audit_db_{STAGE.lower()}"
CONFIG_DB_NAME = f"config_db_{STAGE.lower()}"

S3_LANDING_BUCKET_NAME = f"apg-landing-{ACCOUNT}-{STAGE.lower()}"
S3_PROCESSED_BUCKET_NAME = f"apg-processed-{ACCOUNT}-{STAGE.lower()}"
S3_GLUE_BUCKET_NAME = f"apg-glue-assets-{ACCOUNT}-{STAGE.lower()}"

CONFIG_STAGE_PATH = "stage"

# CONFIG FOR MONTHLY SCHEDULE
MONTHLY_RUN_ON_DAY_SCHEDULE = 2

MONTHLY_SCHEDULE = datetime.date(
    year=TODAY.year, month=TODAY.month, day=MONTHLY_RUN_ON_DAY_SCHEDULE
)

if STAGE == "dev":  # Enables pipeline to run all tasks in dev
    YEARLY_SCHEDULE = TODAY
    MONTHLY_SCHEDULE = TODAY

PIPELINE_META_DIR = "pipeline_meta"
INCOMING_FOLDER_NAME = "incoming"
ALL_INGESTION_DIR_NAME = "all_ef_files"

SUFFIX_FILES_TO_OMIT = [".done", ".completed"]

DATA_PIPELINE = {
    "state_emission_daily.done": {
        "type": "state_emission_daily",
        "incoming_path": f"{INCOMING_FOLDER_NAME}/all_ef_files",
        "create_wf_dependency_done_file": [
            "state_emission_monthly.done",
        ],
        "workflows": [
            {
                "cadence": "daily",
                "monthly": MONTHLY_SCHEDULE,
                "yearly": None,
                "param_store_state_machine_name": "/pipeline/sm-usghg-emission-factor-daily",  # noqa
            }
        ],
        "trigger_statemachine": "true",
        "glue_runtime_sql_params": {
            "param_s3_landing_bucket_name": S3_LANDING_BUCKET_NAME,
            "param_s3_processed_bucket_name": S3_PROCESSED_BUCKET_NAME,
            "param_landing_db_name": LANDING_DB_NAME,
            "param_processed_db_name": PROCESSED_DB_NAME,
        },
        "step_function_payloads": {"key": "value"},
    },
    "state_emission_monthly.done": {
        "type": "usghgemission_monthly",
        "incoming_path": f"{INCOMING_FOLDER_NAME}/all_ef_files",
        "create_wf_dependency_done_file": [],
        "workflows": [
            {
                "cadence": "daily",
                "monthly": MONTHLY_SCHEDULE,
                "yearly": None,
                "param_store_state_machine_name": "/pipeline/sm-usghg-emission-factor-monthly",  # noqa
            }
        ],
        "trigger_statemachine": "true",
        "glue_runtime_sql_params": {
            "param_s3_landing_bucket_name": S3_LANDING_BUCKET_NAME,
            "param_s3_processed_bucket_name": S3_PROCESSED_BUCKET_NAME,
            "param_landing_db_name": LANDING_DB_NAME,
            "param_processed_db_name": PROCESSED_DB_NAME,
        },
        "step_function_payloads": {"key": "value"},
    },
}
