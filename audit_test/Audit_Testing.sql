--Approach of testing 

--triger pipeline for exec_date=2024-11-08 and let both daily and monthly pipiline complete once 
 touch state_emission_daily.done && aws s3 cp state_emission_daily.done s3://apg-landing-038462746512-dev/incoming/all_ef_files/2024-11-08/

--sample record to update 
--647,808  records in utility_data_oh for date('2024-11-08')
 
SELECT * FROM "landing_db_dev"."utility_data_oh" 
where exec_date=date('2024-11-08')
and plant_id_eia=2828 
and  emissions_unit_id_epa='1' 
and operating_datetime_utc=cast('1996-01-01 05:00:00.000' as timestamp);

SELECT * FROM "landing_db_dev"."utility_data_oh" 
where exec_date=date('2024-11-08')
and (plant_id_eia,emissions_unit_id_epa,operating_datetime_utc) not in 
(SELECT plant_id_eia,emissions_unit_id_epa,operating_datetime_utc FROM "landing_db_dev"."utility_data_oh" 
where exec_date=date('2024-11-08')
and plant_id_eia=2828 
and  emissions_unit_id_epa='1' 
and operating_datetime_utc=cast('1996-01-01 05:00:00.000' as timestamp)
)

---statistics before update

select date_format(operating_datetime_utc,'%Y%m') as record_month,
sum(co2_mass_tons)
from landing_db_dev.utility_data_oh
where exec_date=date('2024-11-08')
group by 1 order by 1;

--before update stats
#	record_month	_col1
1	199601	8708463.0
2	199602	8421726.0
3	199603	8709676.0
4	199604	7758302.5
5	199605	7440033.0
6	199606	7880804.0
7	199607	8737553.0
8	199608	9343616.0
9	199609	8195362.0
10	199610	8147022.5
11	199611	8831125.0
12	199612	9107724.0
13	199701	48169.9


select  record_month,co2_ton_oh,co2_ton_in
from processed_db_dev.utility_emissions_monthly
where exec_date=date('2024-11-08')
order by 1;
--before update stats
#	record_month	co2_ton_oh	co2_ton_in
1	199601	8708463.0	5952637.5
2	199602	8421726.0	5484255.5
3	199603	8709676.0	5226433.5
4	199604	7758302.5	5172732.0
5	199605	7440033.0	5763495.5
6	199606	7880804.0	5867209.5
7	199607	8737553.0	6834083.0
8	199608	9343616.0	7014062.0
9	199609	8195362.0	5604975.0
10	199610	8147022.5	5275027.0
11	199611	8831125.0	6332157.0
12	199612	9107724.0	6602754.5
13	199701	48169.9	41402.402



--create back up for test data 
CREATE TABLE landing_db_dev.utility_data_oh_testaudit WITH (
format = 'PARQUET', 
external_location =  's3://apg-landing-038462746512-dev/landing_db_dev/utility_data_oh_testaudit'
) AS 
select 
plant_id_eia,
plant_id_epa,
emissions_unit_id_epa,
operating_datetime_utc,
year,
state,
operating_time_hours,
gross_load_mw,
heat_content_mmbtu,
steam_load_1000_lbs,
so2_mass_lbs,
so2_mass_measurement_code,
nox_mass_lbs,
nox_mass_measurement_code,
co2_mass_tons*10000 as co2_mass_tons,
co2_mass_measurement_code
from 
landing_db_dev.utility_data_oh
where exec_date=date('2024-11-08')
and plant_id_eia=2828 
and  emissions_unit_id_epa='1' 
and operating_datetime_utc=cast('1996-01-01 05:00:00.000' as timestamp)
union
select 
plant_id_eia,
plant_id_epa,
emissions_unit_id_epa,
operating_datetime_utc,
year,
state,
operating_time_hours,
gross_load_mw,
heat_content_mmbtu,
steam_load_1000_lbs,
so2_mass_lbs,
so2_mass_measurement_code,
nox_mass_lbs,
nox_mass_measurement_code,
co2_mass_tons,
co2_mass_measurement_code
from 
landing_db_dev.utility_data_oh
where exec_date=date('2024-11-08')
and (plant_id_eia,emissions_unit_id_epa,operating_datetime_utc) not in 
(SELECT plant_id_eia,emissions_unit_id_epa,operating_datetime_utc FROM "landing_db_dev"."utility_data_oh" 
where exec_date=date('2024-11-08')
and plant_id_eia=2828 
and  emissions_unit_id_epa='1' 
and operating_datetime_utc=cast('1996-01-01 05:00:00.000' as timestamp)
)

select count(*) from landing_db_dev.utility_data_oh_testaudit
--647,808
--original  data 
select sum(co2_mass_tons) from landing_db_dev.utility_data_oh
where exec_date=date('2024-11-08') and date_format(operating_datetime_utc,'%Y%m')='199601';
 --8708463.0
 --new test data
select cast(sum(co2_mass_tons) as decimal(25,10)) from landing_db_dev.utility_data_oh_testaudit
where  date_format(operating_datetime_utc,'%Y%m')='199601';
--1	12368097.0000000000

--So we see an increase of co2 tonne for 199601


-- delete  landing data for utility_data_oh for exec_date=2024-11-08
aws s3 rm s3://apg-landing-038462746512-dev/landing_db_dev/utility_data_oh/exec_date=2024-11-08/ --recursive
aws s3 rm s3://apg-landing-038462746512-dev/incoming/all_ef_files/2024-11-08/epacems-1996-OH.parquet

--copy test data back to incoming folder for date='2024-11-08' for epacems-1996-OH.parquet
SOURCE_FILE=$(aws s3 ls s3://apg-landing-038462746512-dev/landing_db_dev/utility_data_oh_testaudit/ | awk '{print $4}') && aws s3 cp s3://apg-landing-038462746512-dev/landing_db_dev/utility_data_oh_testaudit/$SOURCE_FILE s3://apg-landing-038462746512-dev/incoming/all_ef_files/2024-11-08/epacems-1996-OH.parquet

-- delete  back up test data for utility_data_oh_testaudit for exec_date=2024-11-08
aws s3 rm s3://apg-landing-038462746512-dev/landing_db_dev/utility_data_oh_testaudit/ --recursive
-- DROP TABLE landing_db_dev.utility_data_oh_testaudit;

--triger pipeline for exec_date=2024-11-08
 touch state_emission_daily.done && aws s3 cp state_emission_daily.done s3://apg-landing-038462746512-dev/incoming/all_ef_files/2024-11-08/

--verify
select count(*) from landing_db_dev.utility_data_oh;
--647,808
select cast(sum(co2_mass_tons) as decimal(25,10)) from landing_db_dev.utility_data_oh
where  exec_date=date('2024-11-08') and date_format(operating_datetime_utc,'%Y%m')='199601';
--1	12368097.0000000000



--after monthly complete verify co2 emission for 199601 must have increased 
select   cast(co2_ton_oh as decimal(25,10)) as co2_ton_oh,co2_ton_in
from processed_db_dev.utility_emissions_monthly
where exec_date=date('2024-11-08') and record_month='199601'
 
--before update stats
#	record_month	co2_ton_oh	co2_ton_in
1	199601	8708463.0	5952637.5
--after test data 
#	co2_ton_oh	co2_ton_in
1	12368097.0000000000	5952637.5

 
--verify audit is empty at this point 
SELECT * FROM "audit_db_dev"."audit" limit 10;
--null

--Now kick start process for exec_date='2024-11-09'
aws s3 cp --recursive ./landing_files/ s3://apg-landing-038462746512-dev/incoming/all_ef_files/2024-11-09/
touch state_emission_daily.done && aws s3 cp state_emission_daily.done s3://apg-landing-038462746512-dev/incoming/all_ef_files/2024-11-09/

--verify data  for exec_date=date('2024-11-09')
select cast(sum(co2_mass_tons) as decimal(25,10)) from landing_db_dev.utility_data_oh
where  exec_date=date('2024-11-09') and date_format(operating_datetime_utc,'%Y%m')='199601';
#	_col0
1	8708463.0000000000

--after monthly complete verify co2 emission for exec_date=date('2024-11-09')
select   cast(co2_ton_oh as decimal(25,10)) as co2_ton_oh,co2_ton_in
from processed_db_dev.utility_emissions_monthly
where exec_date=date('2024-11-09') and record_month='199601'
#	co2_ton_oh	co2_ton_in
1	8708463.0000000000	5952637.5


--observation 
for exec_date=date('2024-11-09') , co2_ton_oh is 8708463 but for same data for a different exec_date=date('2024-11-08') it was 12368097
Lets see if this is captured in audit 


--after both daily and monthly process completes   for exec_date='2024-11-09'
verify Audit 

SELECT layer,attribute,grain_key_1,
grain_value_1,grain_key_2,
grain_value_2,grain_key_3,
grain_value_3,
period,
cast(curr_value as decimal(20,2)) as curr_value,
cast(prev_value as decimal(20,2)) as prev_value,
exec_date,
table_name
FROM "audit_db_dev"."audit";

#	layer	attribute	grain_key_1	grain_value_1	grain_key_2	grain_value_2	grain_key_3	grain_value_3	period	curr_value	prev_value	exec_date	table_name
1	landing	co2_mass_tons	plant_id_eia	2828	emissions_unit_id_epa	1	operating_datetime_utc	1996-01-01 05:00:00.000	1996-01-01	366.00	3660000.00	2024-11-09	utility_data_oh
2	processed	co2_ton_total	record_month	199601					1996-01-01	14661100.00	18320734.00	2024-11-09	utility_emissions_monthly
3	processed	co2_ton_oh	record_month	199601					1996-01-01	8708463.00	12368097.00	2024-11-09	utility_emissions_monthly


This tells that co2_ton_total decreased between 11/9 and 11/8 exec date.
thats because of co2_ton_oh decreased between 11/9 and 11/8 exec date.
Thats because co2_mass_tons decreased between 11/9 and 11/8 exec date for plant_id_eia=2828,emissions_unit_id_ea='1' and for operating_datetime_utc='1996-01-01 05:00:00.000', it became 366 from 3660000








