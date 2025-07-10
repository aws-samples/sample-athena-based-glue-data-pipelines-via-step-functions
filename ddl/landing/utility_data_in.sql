CREATE EXTERNAL TABLE  landing_db_dev.utility_data_in(
  plant_id_eia int, 
  plant_id_epa int, 
  emissions_unit_id_epa string, 
  operating_datetime_utc timestamp, 
  year int, 
  state string, 
  operating_time_hours float, 
  gross_load_mw float, 
  heat_content_mmbtu float, 
  steam_load_1000_lbs float, 
  so2_mass_lbs float, 
  so2_mass_measurement_code string, 
  nox_mass_lbs float, 
  nox_mass_measurement_code string, 
  co2_mass_tons float, 
  co2_mass_measurement_code string)
  PARTITIONED BY (
  exec_date date)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
's3://apg-landing-$CDK_DEFAULT_ACCOUNT-dev/landing_db_dev/utility_data_in/'
;
 
msck repair table landing_db_dev.utility_data_in;