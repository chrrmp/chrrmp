--STEP 1

TRUNCATE table al_finance.fin_tne_itinerary_ref_ds;
--STEP 2

INSERT INTO al_finance.fin_tne_itinerary_ref_ds
(
	emp_name,
	emp_id,
	company_cd,
	cost_object,
	ledger_cd,
	account_cd,
	report_id,
	trip_start_dt,
	trip_start_city,
	trip_start_country,
	trip_end_dt,
	trip_end_city,
	trip_end_country,
	travel_duration_in_hrs,
	travel_duration_in_days,
	travel_type,
	travel_cd,
	src_system_id,
	src_name,
	dl_ins_by,
	dl_upd_by,
	dl_ins_ts,
	dl_upd_ts
)
SELECT
	emp_name,
	emp_id,
	company_cd,
	cost_object,
	ledger_cd,
	account_cd,
	report_id,
	to_date(trip_start_date,'YYYY-MM-DD') AS trip_start_dt,
	trip_start_city,
	trip_start_country,
	to_date(trip_end_date,'YYYY-MM-DD') AS trip_end_dt,
	trip_end_city,
	trip_end_country,
	travel_duration_in_hrs,
	travel_duration_in_days,
	travel_type,
	travel_cd,
	COALESCE(report_id,'')||'~'||to_char(to_date(trip_start_date,'YYYY-MM-DD'),'YYYY-MM-DD') as src_system_id,
	'CON' AS src_name,
	'fin_tne_mtd_etl_scripts~tne_con_dim_stg' AS dl_ins_by,
	'fin_tne_mtd_etl_scripts~tne_con_dim_stg' AS dl_upd_by,
	current_timestamp AS dl_ins_ts,
	current_timestamp AS dl_upd_ts
FROM 
	og_file_src.fin_tne_itinerary_extract;
