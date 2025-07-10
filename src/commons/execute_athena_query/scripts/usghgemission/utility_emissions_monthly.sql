insert into {{ param_processed_db_name }}.utility_emissions_monthly with oh_emissions as (
		select date_format(operating_datetime_utc, '%Y%m') as record_month,
			sum(co2_mass_tons) as co2_ton
		from {{ param_landing_db_name }}.utility_data_oh
		where exec_date = date('{{ param_execution_date }}')
		group by 1
	),
	in_emissions as (
		select date_format(operating_datetime_utc, '%Y%m') as record_month,
			sum(co2_mass_tons) as co2_ton
		from {{ param_landing_db_name }}.utility_data_in
		where exec_date = date('{{ param_execution_date }}')
		group by 1
	)
select coalesce(oh.record_month, rr.record_month) as record_month,
	oh.co2_ton as co2_ton_oh,
	rr.co2_ton as co2_ton_in,
	(oh.co2_ton + rr.co2_ton) as co2_ton_total,
	date('{{ param_execution_date }}') as exec_date
from oh_emissions oh
	full outer join in_emissions rr on oh.record_month = rr.record_month