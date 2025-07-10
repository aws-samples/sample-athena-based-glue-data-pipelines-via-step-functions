CREATE EXTERNAL TABLE processed_db_dev.utility_emissions_daily(
  record_date date, 
  co2_ton_oh float, 
  co2_ton_in float, 
  co2_ton_total float)
  PARTITIONED BY (
  exec_date date)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://apg-processed-$CDK_DEFAULT_ACCOUNT-dev/processed_db_dev/utility_emissions_daily'