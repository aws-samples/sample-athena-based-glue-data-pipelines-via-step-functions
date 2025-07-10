from pipeline_stacks import pipeline_config as pipe_cfg

UPDATE_LANDING_PARTITION = [
    {
        "db_name": pipe_cfg.LANDING_DB_ATTRIBUTES,
        "tables": ["update_landing_partition"],
        "function": "job",
    }
]

UTILITY_EMISSION_DAILY = [
    {
        "db_name": pipe_cfg.PROCESSED_DB_ATTRIBUTES,
        "tables": ["utility_emissions_daily"],
        "function": "job",
    }
]
