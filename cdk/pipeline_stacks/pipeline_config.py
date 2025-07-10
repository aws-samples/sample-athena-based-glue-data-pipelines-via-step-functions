import config as cfg

JINJA2_WHL_NAME = "Jinja2-3.1.2-py3-none-any.whl"
JINJA2_WHL_S3_PREFIX = f"whl/{JINJA2_WHL_NAME}"

WRANGLER_WHL_NAME = (
    "awswrangler-3.5.1-py3-none-any.whl"  # "awswrangler-3.4.2-py3-none-any.whl"
)
WRANGLER_WHL_S3_PREFIX = f"whl/{WRANGLER_WHL_NAME}"

SQL_J2 = {
    "all_others": "templated_audit.jinja2.sql",
}

# AUDIT DB CONFIG
EX_LANDING_TB_AUDIT_DB_ATTRIBUTES = {
    "exec_db": cfg.LANDING_DB_NAME,
    "table_db": cfg.AUDIT_DB_NAME,
}
EX_PROCESSED_TB_AUDIT_DB_ATTRIBUTES = {
    "exec_db": cfg.PROCESSED_DB_NAME,
    "table_db": cfg.AUDIT_DB_NAME,
}
LANDING_DB_ATTRIBUTES = {
    "exec_db": cfg.LANDING_DB_NAME,
    "table_db": cfg.LANDING_DB_NAME,
}

PROCESSED_DB_ATTRIBUTES = {
    "exec_db": cfg.PROCESSED_DB_NAME,
    "table_db": cfg.PROCESSED_DB_NAME,
}

GLUE_JOBS_RETRY = 0
GLUE_TASK_RETRY = 2
LAMBDA_TASK_RETRY = 1
WAITING_TIME_BEFORE_RETRY = 60
