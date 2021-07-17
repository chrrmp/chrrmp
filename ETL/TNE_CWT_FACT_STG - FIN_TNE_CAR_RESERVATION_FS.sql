--STEP 1

TRUNCATE TABLE AL_FINANCE.FIN_TNE_CAR_RESERVATION_FS;
--STEP 2

INSERT into AL_FINANCE.FIN_TNE_CAR_RESERVATION_FS(emp_id
	,base_reportng_cd
	,issuing_cntry_name
	,traveler_name
	,car_supplier_cd
	,car_supplier_name
	,obt_action_cd
	,obt_vendor_cd
	,agent_assist_rc
	,booking_dt
	,issue_dt
	,confirmation_nbr
	,invoice_voucher_nbr
	,pick_up_city_cd
	,pick_up_city_name
	,pick_up_cntry_name
	,pick_up_regn_name
	,drop_off_city_cd
	,drop_off_city_name
	,drop_off_cntry_name
	,drop_off_regn_name
	,pick_up_dt
	,pick_up_time
	,drop_off_dt
	,drop_off_time
	,car_only
	,car_type
	,rate_duration
	,rental_rt
	,rate_multiplier
	,nbr_of_cars
	,car_days
	,total_car_cost
	,lowest_rate
	,ref_rate
	,local_curncy_cd
	,local_curncy_rt
	,invoice_referral
	,rc_missed_local_cd
	,rc_missed_local_desc
	,rc_realized_local_cd
	,rc_realized_local_desc
	,rc_missed_global_cd
	,rc_missed_global_desc
	,rc_realized_global_cd
	,rc_realized_global_desc
	,trip_type
	,branch_id
	,agent_id
	,booking_iata_nbr
	,back_office_txn_id
	,pnr
	,gds
	,travel_purpose
	,accnt_nbr
	,pos_fee_amt
	,hier_1
	,hier_2
	,hier_3
	,hier_4
	,hier_5
	,client_defined_1
	,client_defined_2
	,client_defined_3
	,client_defined_4
	,client_defined_5
	,client_defined_6
	,client_defined_7
	,client_defined_8
	,client_defined_9
	,client_defined_10
	,client_defined_11
	,client_defined_12
	,client_defined_13
	,client_defined_14
	,client_defined_15
	,client_defined_16
	,client_defined_17
	,client_defined_18
	,client_defined_19
	,client_defined_20
	,src_last_upd_dt
	,delete_record
	,src_system_cd
	,src_system_id
	,src_name
	,dl_ins_by
	,dl_upd_by
	,dl_ins_ts
	,dl_upd_ts)
select 
	 client_defined_field_6
	,base_reporting_code
	,issuing_country_name
	,traveler_name
	,car_supplier_code
	,car_supplier_name
	,obt_action_code
	,obt_vendor_code
	,agent_assist_rc
	,to_date(booking_date, 'DD-MON-YY')
	,to_date(issue_date, 'DD-MON-YYYY')
	,confirmation_number
	,invoice_voucher_number
	,pick_up_city
	,pick_up_city_name
	,pick_up_country_name
	,pick_up_region_name
	,drop_off_city
	,drop_off_city_name
	,drop_off_country_name
	,drop_off_region_name
	,to_date(pick_up_date, 'DD-MON-YYYY')
	,pick_up_time
	,to_date(drop_off_date, 'DD-MON-YYYY')
	,drop_off_time
	,car_only
	,car_type
	,rate_duration
	,CAST(rental_rate AS NUMERIC)
	,CAST(rate_multiplier AS NUMERIC)
	,CAST(number_of_cars AS NUMERIC)
	,CAST(car_days AS NUMERIC)
	,CAST(total_car_cost AS NUMERIC)
	,CAST(lowest_rate AS NUMERIC)
	,CAST(reference_rate AS NUMERIC)
	,local_currency_code
	,CAST(local_currency_rate AS NUMERIC)
	,invoice_referral
	,rc_missed_local
	,rc_missed_local_description
	,rc_realized_local
	,rc_realized_local_description
	,rc_missed_global
	,rc_missed_global_description
	,rc_realized_global
	,rc_realized_global_description
	,trip_type
	,branch_id
	,agent_id
	,booking_iata_number
	,backoffice_tran_id
	,pnr
	,gds
	,travel_purpose
	,account_number
	,CAST(point_of_sale_fee AS NUMERIC)
	,hierarchy_1
	,hierarchy_2
	,hierarchy_3
	,hierarchy_4
	,hierarchy_5
	,client_defined_field_1
	,client_defined_field_2
	,client_defined_field_3
	,client_defined_field_4
	,client_defined_field_5
	,client_defined_field_6
	,client_defined_field_7
	,client_defined_field_8
	,client_defined_field_9
	,client_defined_field_10
	,client_defined_field_11
	,client_defined_field_12
	,client_defined_field_13
	,client_defined_field_14
	,client_defined_field_15
	,client_defined_field_16
	,client_defined_field_17
	,client_defined_field_18
	,client_defined_field_19
	,client_defined_field_20
	,to_date(last_update_date,'DD-MON-YYYY')
	,delete_record
	,source_system_code
	,backoffice_tran_id||'~'||'CWT'
	,'CWT'
	,'fin_tne_mtd_etl_scripts~tne_cwt_fact_stg'
	,'fin_tne_mtd_etl_scripts~tne_cwt_fact_stg'
	,current_timestamp
	,current_timestamp 
	from 
	OG_FILE_SRC.FIN_TNE_CAR_RESERVATION;
