CREATE EXTERNAL TABLE audit_db_dev.audit(
  layer varchar(50),
  attribute varchar(100),
  grain_key_1 varchar(100),
  grain_value_1 varchar(100),
  grain_key_2 varchar(100),
  grain_value_2 varchar(100),
  grain_key_3 varchar(100),
  grain_value_3 varchar(100),
  grain_key_4 varchar(100),
  grain_value_4 varchar(100),
  grain_key_5 varchar(100),
  grain_value_5 varchar(100),
  period date,
  curr_value varchar(100),
  prev_value varchar(100)
  )
PARTITIONED BY (
  pipeline varchar(50),
  exec_date date,
  table_name varchar(100),
  time_grain varchar(50)
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
's3://apg-audit-$CDK_DEFAULT_ACCOUNT-dev/audit_db_dev/audit/'
;


msck repair table audit;
