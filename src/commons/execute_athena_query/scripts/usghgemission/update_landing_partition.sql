ALTER TABLE {{ param_landing_db_name }}.utility_data_in
ADD IF NOT EXISTS PARTITION (exec_date = '{{ param_execution_date }}')
LOCATION 's3://{{ param_s3_landing_bucket_name }}/{{ param_landing_db_name }}/utility_data_in/exec_date={{ param_execution_date }}/' ;

ALTER TABLE {{ param_landing_db_name }}.utility_data_oh
ADD IF NOT EXISTS PARTITION (exec_date = '{{ param_execution_date }}')
LOCATION 's3://{{ param_s3_landing_bucket_name }}/{{ param_landing_db_name }}/utility_data_oh/exec_date={{ param_execution_date }}/' ;

