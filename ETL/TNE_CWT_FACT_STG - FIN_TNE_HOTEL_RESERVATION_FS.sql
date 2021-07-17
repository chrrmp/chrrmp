--STEP 1

TRUNCATE TABLE AL_FINANCE.FIN_TNE_HOTEL_RESERVATION_FS;
--STEP 2

INSERT INTO AL_FINANCE.FIN_TNE_HOTEL_RESERVATION_FS
(
	 emp_id
	,base_reportng_cd
	,base_reportng_name
	,org_id
	,org_name
	,issuing_cntry_name
	,traveler_name
	,super_chain_name
	,hotel_chain_cd
	,hotel_chain_name
	,property_name
	,street_address
	,hotel_city
	,hotel_state
	,hotel_zip
	,hotel_cntry
	,hotel_phone_nbr
	,invoice_referral
	,invoice_voucher_nbr
	,obt_action_cd
	,obt_vendor_cd
	,obt_vendor_name
	,obt_vendor_type_name
	,agent_assist_rc
	,booking_method
	,booking_dt
	,issue_dt
	,confirmation_nbr
	,in_dt
	,out_dt
	,metro_city_cd
	,near_metro_city_name
	,near_airport_cd
	,near_airport_name
	,region
	,hotel_only
	,room_type
	,rate_type
	,rate_duration
	,room_rt
	,multiplier_rt
	,nbr_of_rooms
	,room_nights
	,total_hotel_cost
	,supplemental_charges_amt
	,tot_hotel_charges_amt
	,ref_rt
	,ref_cost
	,realized_savings_amt
	,realized_savings_percent
	,lowest_rt
	,low_cost
	,missed_savings_amt
	,missed_saving_percent
	,potential_saivings_amt
	,potential_savings_percent
	,local_curncy_cd
	,local_curncy_rt
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
	,harp_cd
	,original_hotel_key
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
	,hotel_preferd_compliance
	,src_last_upd_dt
	,delete_record
	,src_system_cd
	,src_system_id
	,src_name
	,dl_ins_by
	,dl_upd_by
	,dl_ins_ts
	,dl_upd_ts)
SELECT 
	client_defined_field_6
	,base_reporting_code
	,base_reporting_name
	,organization_id
	,organization
	,issuing_country_name
	,traveler_name
	,super_chain
	,hotel_chain_code
	,hotel_chain_name
	,property_name
	,street_address
	,hotel_city
	,hotel_state
	,hotel_zip
	,hotel_country
	,hotel_phone_number
	,invoice_referral
	,invoice_voucher_number
	,obt_action_code
	,obt_vendor_code
	,obt_vendor_name
	,obt_vendor_type_name
	,agent_assist_rc
	,booking_method
	,TO_TIMESTAMP(booking_date,'DD-MON-YYYY')
	,TO_TIMESTAMP(issue_date,'DD-MON-YYYY')
	,confirmation_number
	,TO_TIMESTAMP(in_date,'DD-MON-YYYY')
	,TO_TIMESTAMP(out_date,'DD-MON-YYYY')
	,metro_city_code
	,nearest_metro_city
	,nearest_airport_code
	,nearest_airport_name
	,region
	,hotel_only
	,room_type
	,rate_type
	,rate_duration
	,CAST(room_rate AS NUMERIC)
	,CAST(rate_multiplier AS NUMERIC)
	,CAST(number_of_rooms AS NUMERIC)
	,CAST(room_nights AS NUMERIC)
	,CAST(total_hotel_cost AS NUMERIC)
	,CAST(supplemental_charges AS NUMERIC)
	,CAST(total_hotel_charges AS NUMERIC)
	,CAST(reference_rate AS NUMERIC)
	,CAST(reference_cost AS NUMERIC)
	,CAST(realized_savings AS NUMERIC)
	,CAST(perc_realized_savings AS NUMERIC)
	,CAST(lowest_rate AS NUMERIC)
	,CAST(low_cost AS NUMERIC)
	,CAST(missed_savings AS NUMERIC)
	,CAST(perc_missed_savings AS NUMERIC)
	,CAST(potential_savings AS NUMERIC)
	,CAST(perc_potential_savings AS NUMERIC)
	,local_currency_code
	,CAST(local_currency_rate AS NUMERIC)
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
	,harp_code
	,original_hotel_key
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
	,hotel_preferred_compliance
	,TO_TIMESTAMP(last_update_date,'DD-MON-YYYY')
	,delete_record
	,source_system_code
	,backoffice_tran_id||'~'||'CWT'
	,'CWT'
	,'fin_tne_mtd_etl_scripts~tne_cwt_fact_stg'
	,'fin_tne_mtd_etl_scripts~tne_cwt_fact_stg'
	,current_timestamp
	,current_timestamp 
FROM OG_FILE_SRC.FIN_TNE_HOTEL_RESERVATION;
