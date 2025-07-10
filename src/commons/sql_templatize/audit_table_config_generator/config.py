import os

STAGE = os.environ.get("STAGE", "dev")
REGION = os.environ.get("REGION", "us-east-1")
ACCOUNT = os.environ.get("ACCOUNT", "123456789012")

S3_GLUE_ASSET_BUCKET = f"apg-glue-assets-{ACCOUNT}-{STAGE}"
TEMPLATIZED_TABLE_CONFIG_S3_PREFIX = "audit/templatized_table_config"
GENERATED_TABLE_CONFIG_S3_PREFIX = "audit/generated_table_config"

LANDING_DB_NAME = f"landing_db_{STAGE}"
PROCESSED_DB_NAME = f"processed_db_{STAGE}"
PUBLISHED_DB_NAME = f"published_db_{STAGE}"
AUDIT_DB_NAME = f"audit_db_{STAGE}"

TABLE_CONFIGS = [
    {
        "pipeline_name": "usghgemission_monthly",
        "frequency": ["daily_and_monthly"],
        "pipeline_type": "usghgemission_monthly",
        "table_configs": [
            {
                "param_exec_date": "",
                "param_stage": f"{STAGE.lower()}",
                "param_layer": "processed",
                "param_audited_table_name": "utility_emissions_monthly",
                "param_monthly_results_table_name": "utility_emissions_monthly",
                "param_month_column_name": "record_month",
                "param_pipeline_name": "usghgemission_monthly",
                "param_grain": "monthly",
                "sql_template_path": "",
                "audited_attributes": [
                    "co2_ton_oh",
                    "co2_ton_in",
                    "co2_ton_total",
                ],
            },
            {
                "param_exec_date": "",
                "param_stage": f"{STAGE.lower()}",
                "param_layer": "landing",
                "param_audited_table_name": "utility_data_oh",
                "param_monthly_results_table_name": "utility_emissions_monthly",
                "param_month_column_name": "b.operating_datetime_utc",
                "param_pipeline_name": "usghgemission_monthly",
                "param_grain": "monthly",
                "sql_template_path": "",
                "audited_attributes": ["co2_mass_tons"],
            },
            {
                "param_exec_date": "",
                "param_stage": f"{STAGE.lower()}",
                "param_layer": "landing",
                "param_audited_table_name": "utility_data_in",
                "param_monthly_results_table_name": "utility_emissions_monthly",
                "param_month_column_name": "b.operating_datetime_utc",
                "param_pipeline_name": "usghgemission_monthly",
                "param_grain": "monthly",
                "sql_template_path": "",
                "audited_attributes": ["co2_mass_tons"],
            },
        ],
    },
]
