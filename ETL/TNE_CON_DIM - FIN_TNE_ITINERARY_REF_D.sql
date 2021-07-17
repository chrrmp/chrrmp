--STEP 1

DELETE FROM al_finance.fin_tne_itinerary_ref_d
	WHERE src_system_id IN (SELECT src_system_id from al_finance.fin_tne_itinerary_ref_ds);
--STEP 2

INSERT INTO al_finance.fin_tne_itinerary_ref_d
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
	'fin_tne_mtd_etl_scripts~tne_con_dim' AS dl_ins_by,
	'fin_tne_mtd_etl_scripts~tne_con_dim' AS dl_upd_by,
	current_timestamp AS dl_ins_ts,
	current_timestamp AS dl_upd_ts
FROM 
	al_finance.fin_tne_itinerary_ref_ds;
