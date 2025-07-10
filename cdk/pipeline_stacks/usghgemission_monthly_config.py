from pipeline_stacks import pipeline_config as pipe_cfg

UPDATE_LANDING_PARTITION = [
    {
        "db_name": pipe_cfg.LANDING_DB_ATTRIBUTES,
        "tables": ["update_landing_partition"],
        "function": "job",
    }
]

UTILITY_EMISSION_MONTHLY = [
    {
        "db_name": pipe_cfg.PROCESSED_DB_ATTRIBUTES,
        "tables": ["utility_emissions_monthly"],
        "function": "job",
    }
]

MONTHLY_AUDIT_TABLES = [
    {
        "db_name": pipe_cfg.EX_LANDING_TB_AUDIT_DB_ATTRIBUTES,
        "tables": [
            "utility_data_in",
            "utility_data_oh",
        ],
        "function": "audit",
    },
    {
        "db_name": pipe_cfg.EX_PROCESSED_TB_AUDIT_DB_ATTRIBUTES,
        "tables": [
            "utility_emissions_monthly",
        ],
        "function": "audit",
    },
]
