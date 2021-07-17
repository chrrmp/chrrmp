--STEP 1

DELETE FROM AL_FINANCE.FIN_TNE_AIR_RESERVATION_F fact 
	using (select * from AL_FINANCE.FIN_TNE_AIR_RESERVATION_FS STAGE where STAGE.delete_record = '0') stage
	where fact.src_system_id=stage.SRC_SYSTEM_ID;
--STEP 2

INSERT INTO AL_FINANCE.FIN_TNE_AIR_RESERVATION_F
(
	emp_id
	,base_reportng_cd
	,issuing_cntry_name
	,traveler_name
	,airline_alliance
	,validatng_airline
	,supplier_name
	,travel_type_desc
	,prod_type_desc
	,eticket_flag
	,obt_action_cd
	,obt_vendor_cd
	,agent_assist_rc
	,tickets_used
	,paid_fare_amt
	,ref_fare_amt
	,realized_savings_amt
	,realized_savings_percent
	,lowest_fare_amt
	,missed_savings_amt
	,missed_saving_percent
	,potential_saivings_amt
	,potential_savings_percent
	,economy_compare_fare
	,rc_missed_local_cd
	,rc_missed_local_desc
	,rc_realized_local_cd
	,rc_realized_local_desc
	,rc_missed_global_cd
	,rc_missed_global_desc
	,rc_realized_global_cd
	,rc_realized_global_desc
	,base_fare_amt
	,tax_amt
	,hst_tax_amt
	,gst_tax_amt
	,qst_tax_amt
	,other_tax_amt
	,pos_fee_amt
	,main_airline_cd
	,service_class_cd
	,service_class_desc
	,ticket_class_cd
	,ticket_class_desc
	,ticket_predominant_class
	,fare_basis_cd
	,air_booking_cd
	,tour_cd
	,ticket_designator
	,ticket_nbr
	,booking_dt
	,invoice_nbr
	,issue_dt
	,ticket_depart_dt
	,ticket_return_dt
	,trip_days
	,advance_purchase
	,advance_booking
	,ticket_routing
	,od_airport_pair
	,origin_airport_cd
	,origin_city_name
	,origin_cntry_name
	,origin_regn_name
	,destn_airport_cd
	,destn_city_name
	,dest_cntry_name
	,dest_regn_name
	,routing_type_desc
	,nbr_of_sub_trips
	,trip_miles
	,sub_trip_miles
	,flight_duration_min
	,total_travel_time
	,trip_type
	,txn_type
	,ticket_status_cd
	,days_to_refund
	,branch_id
	,agent_id
	,booking_iata_nbr
	,ticketing_iata_nbr
	,back_office_txn_id
	,form_of_payment_cd
	,credit_card_cd
	,credit_card_nbr
	,pnr
	,gds
	,travel_purpose
	,accnt_nbr
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
	,local_curncy_cd
	,local_paid_fare
	,co2e_kg
	,kilometers
	,main_airline_name
	,tickets_issued
	,txn_cnt
	,client_master_cd_name
	,booking_method_desc
	,nbr_of_coupons
	,org_id
	,org_name
	,orig_issue_dt
	,orig_paid_fare
	,orig_ticket_nbr
	,orig_state_prov_cd
	,dest_state_prov_cd
	,src_last_upd_dt
	,delete_record
	,src_system_cd
	,od_trip_type
	,src_system_id
	,src_name
	,dl_ins_by
	,dl_upd_by
	,dl_ins_ts
	,dl_upd_ts
	,business_segment,
	sub_business,
	emp_org,
	sso_id,
	workday_user_name,
	emp_full_name,
	email_address,
	emp_cntry,
	job_family_grp,
	job_family,
	job_name,
	business_grp_name,
	emp_func,
	emp_home_regn,
	operatng_ledger_buc,
	operatng_ledger_cc_cd,
	operatng_ledger_compny_cd,
	operatng_ledger_pl_cd,
	office_address,
	band_grp,
	supervisor_id_lvl_01,
	supervisor_name_lvl_01,
	supervisor_id_lvl_02,
	supervisor_name_lvl_02,
	supervisor_id_lvl_03,
	supervisor_name_lvl_03,
	supervisor_id_lvl_04,
	supervisor_name_lvl_04,
	supervisor_id_lvl_05,
	supervisor_name_lvl_05,
	supervisor_id_lvl_06,
	supervisor_name_lvl_06,
	supervisor_id_lvl_07,
	supervisor_name_lvl_07,
	supervisor_id_lvl_08,
	supervisor_name_lvl_08,
	supervisor_id_lvl_09,
	supervisor_name_lvl_09,
	supervisor_id_lvl_10,
	supervisor_name_lvl_10
)
select 
	t.emp_id
	,t.base_reportng_cd
	,t.issuing_cntry_name
	,t.traveler_name
	,t.airline_alliance
	,t.validatng_airline
	,t.supplier_name
	,t.travel_type_desc
	,t.prod_type_desc
	,t.eticket_flag
	,t.obt_action_cd
	,t.obt_vendor_cd
	,t.agent_assist_rc
	,t.tickets_used
	,t.paid_fare_amt
	,t.ref_fare_amt
	,t.realized_savings_amt
	,t.realized_savings_percent
	,t.lowest_fare_amt
	,t.missed_savings_amt
	,t.missed_saving_percent
	,t.potential_saivings_amt
	,t.potential_savings_percent
	,t.economy_compare_fare
	,t.rc_missed_local_cd
	,t.rc_missed_local_desc
	,t.rc_realized_local_cd
	,t.rc_realized_local_desc
	,t.rc_missed_global_cd
	,t.rc_missed_global_desc
	,t.rc_realized_global_cd
	,t.rc_realized_global_desc
	,t.base_fare_amt
	,t.tax_amt
	,t.hst_tax_amt
	,t.gst_tax_amt
	,t.qst_tax_amt
	,t.other_tax_amt
	,t.pos_fee_amt
	,t.main_airline_cd
	,t.service_class_cd
	,t.service_class_desc
	,t.ticket_class_cd
	,t.ticket_class_desc
	,t.ticket_predominant_class
	,t.fare_basis_cd
	,t.air_booking_cd
	,t.tour_cd
	,t.ticket_designator
	,t.ticket_nbr
	,t.booking_dt
	,t.invoice_nbr
	,t.issue_dt
	,t.ticket_depart_dt
	,t.ticket_return_dt
	,t.trip_days
	,t.advance_purchase
	,t.advance_booking
	,t.ticket_routing
	,t.od_airport_pair
	,t.origin_airport_cd
	,t.origin_city_name
	,t.origin_cntry_name
	,t.origin_regn_name
	,t.destn_airport_cd
	,t.destn_city_name
	,t.dest_cntry_name
	,t.dest_regn_name
	,t.routing_type_desc
	,t.nbr_of_sub_trips
	,t.trip_miles
	,t.sub_trip_miles
	,t.flight_duration_min
	,t.total_travel_time
	,t.trip_type
	,t.txn_type
	,t.ticket_status_cd
	,t.days_to_refund
	,t.branch_id
	,t.agent_id
	,t.booking_iata_nbr
	,t.ticketing_iata_nbr
	,t.back_office_txn_id
	,t.form_of_payment_cd
	,t.credit_card_cd
	,t.credit_card_nbr
	,t.pnr
	,t.gds
	,t.travel_purpose
	,t.accnt_nbr
	,t.hier_1
	,t.hier_2
	,t.hier_3
	,t.hier_4
	,t.hier_5
	,t.client_defined_1
	,t.client_defined_2
	,t.client_defined_3
	,t.client_defined_4
	,t.client_defined_5
	,t.client_defined_6
	,t.client_defined_7
	,t.client_defined_8
	,t.client_defined_9
	,t.client_defined_10
	,t.client_defined_11
	,t.client_defined_12
	,t.client_defined_13
	,t.client_defined_14
	,t.client_defined_15
	,t.client_defined_16
	,t.client_defined_17
	,t.client_defined_18
	,t.client_defined_19
	,t.client_defined_20
	,t.local_curncy_cd
	,t.local_paid_fare
	,t.co2e_kg
	,t.kilometers
	,t.main_airline_name
	,t.tickets_issued
	,t.txn_cnt
	,t.client_master_cd_name
	,t.booking_method_desc
	,t.nbr_of_coupons
	,t.org_id
	,t.org_name
	,t.orig_issue_dt
	,t.orig_paid_fare
	,t.orig_ticket_nbr
	,t.orig_state_prov_cd
	,t.dest_state_prov_cd
	,t.src_last_upd_dt
	,t.delete_record
	,t.src_system_cd
	,t.od_trip_type
	,t.src_system_id
	,t.src_name
	,'fin_tne_mtd_etl_scripts~fin_cwt_fact'
	,'fin_tne_mtd_etl_scripts~fin_cwt_fact'
	,current_timestamp
	,current_timestamp
	,emp.business_segment AS business_segment,
	emp.sub_business as sub_business,
	emp.assignment_attrib_10 AS emp_org,
	emp.sso_id as SSO_ID,
	emp.workday_user_name,
	emp.emp_full_name,
	emp.email_address as email_address,
	emp.cntry_cd AS emp_cntry,
	emp.job_family_grp as job_family_grp,
	emp.job_family,
	emp.job_name,
	emp.business_grp_name,
	emp.emp_func,
	emp.emp_home_regn as emp_home_regn,
	emp.operatng_ledger_buc,
	emp.operatng_ledger_cc_cd as operatng_ledger_cc_cd,
	emp.operatng_ledger_compny_cd as operatng_ledger_compny_cd,
	emp.operatng_ledger_pl_cd,
	emp.office_address,
	emp.band_grp,
	emp.supervisor_id_lvl_01,
	emp.supervisor_name_lvl_01,
	emp.supervisor_id_lvl_02,
	emp.supervisor_name_lvl_02,
	emp.supervisor_id_lvl_03,
	emp.supervisor_name_lvl_03,
	emp.supervisor_id_lvl_04,
	emp.supervisor_name_lvl_04,
	emp.supervisor_id_lvl_05,
	emp.supervisor_name_lvl_05,
	emp.supervisor_id_lvl_06,
	emp.supervisor_name_lvl_06,
	emp.supervisor_id_lvl_07,
	emp.supervisor_name_lvl_07,
	emp.supervisor_id_lvl_08,
	emp.supervisor_name_lvl_08,
	emp.supervisor_id_lvl_09,
	emp.supervisor_name_lvl_09,
	emp.supervisor_id_lvl_10,
	emp.supervisor_name_lvl_10
FROM   
(select a.* ,row_number() over (partition by src_system_id order by src_last_upd_dt DESC) as rn FROM
AL_FINANCE.FIN_TNE_AIR_RESERVATION_FS a WHERE a.delete_record = '0' ) t 
LEFT JOIN al_finance.fin_employee_d emp ON t.emp_id = emp.src_system_id
WHERE t.rn=1
AND delete_record = '0';
--STEP 3

CREATE TEMP TABLE TEMP_AIR_RESERVATIONS
 AS (SELECT fs_deleted.* FROM (SELECT t.*,row_number() over(partition by src_system_id order by src_system_id) as rn 
					from AL_FINANCE.FIN_TNE_AIR_RESERVATION_FS t  where delete_record = '1') fs_deleted WHERE fs_deleted.rn = 1) DISTRIBUTED BY (src_system_id);
--STEP 4

UPDATE AL_FINANCE.FIN_TNE_AIR_RESERVATION_F F
SET delete_record = '1'
FROM TEMP_AIR_RESERVATIONS fs_temp
WHERE fs_temp.src_system_id = f.src_system_id;
--STEP 5

INSERT INTO AL_FINANCE.FIN_TNE_AIR_RESERVATION_F
(
	emp_id
	,base_reportng_cd
	,issuing_cntry_name
	,traveler_name
	,airline_alliance
	,validatng_airline
	,supplier_name
	,travel_type_desc
	,prod_type_desc
	,eticket_flag
	,obt_action_cd
	,obt_vendor_cd
	,agent_assist_rc
	,tickets_used
	,paid_fare_amt
	,ref_fare_amt
	,realized_savings_amt
	,realized_savings_percent
	,lowest_fare_amt
	,missed_savings_amt
	,missed_saving_percent
	,potential_saivings_amt
	,potential_savings_percent
	,economy_compare_fare
	,rc_missed_local_cd
	,rc_missed_local_desc
	,rc_realized_local_cd
	,rc_realized_local_desc
	,rc_missed_global_cd
	,rc_missed_global_desc
	,rc_realized_global_cd
	,rc_realized_global_desc
	,base_fare_amt
	,tax_amt
	,hst_tax_amt
	,gst_tax_amt
	,qst_tax_amt
	,other_tax_amt
	,pos_fee_amt
	,main_airline_cd
	,service_class_cd
	,service_class_desc
	,ticket_class_cd
	,ticket_class_desc
	,ticket_predominant_class
	,fare_basis_cd
	,air_booking_cd
	,tour_cd
	,ticket_designator
	,ticket_nbr
	,booking_dt
	,invoice_nbr
	,issue_dt
	,ticket_depart_dt
	,ticket_return_dt
	,trip_days
	,advance_purchase
	,advance_booking
	,ticket_routing
	,od_airport_pair
	,origin_airport_cd
	,origin_city_name
	,origin_cntry_name
	,origin_regn_name
	,destn_airport_cd
	,destn_city_name
	,dest_cntry_name
	,dest_regn_name
	,routing_type_desc
	,nbr_of_sub_trips
	,trip_miles
	,sub_trip_miles
	,flight_duration_min
	,total_travel_time
	,trip_type
	,txn_type
	,ticket_status_cd
	,days_to_refund
	,branch_id
	,agent_id
	,booking_iata_nbr
	,ticketing_iata_nbr
	,back_office_txn_id
	,form_of_payment_cd
	,credit_card_cd
	,credit_card_nbr
	,pnr
	,gds
	,travel_purpose
	,accnt_nbr
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
	,local_curncy_cd
	,local_paid_fare
	,co2e_kg
	,kilometers
	,main_airline_name
	,tickets_issued
	,txn_cnt
	,client_master_cd_name
	,booking_method_desc
	,nbr_of_coupons
	,org_id
	,org_name
	,orig_issue_dt
	,orig_paid_fare
	,orig_ticket_nbr
	,orig_state_prov_cd
	,dest_state_prov_cd
	,src_last_upd_dt
	,delete_record
	,src_system_cd
	,od_trip_type
	,src_system_id
	,src_name
	,dl_ins_by
	,dl_upd_by
	,dl_ins_ts
	,dl_upd_ts
	,business_segment,
	sub_business,
	emp_org,
	sso_id,
	workday_user_name,
	emp_full_name,
	email_address,
	emp_cntry,
	job_family_grp,
	job_family,
	job_name,
	business_grp_name,
	emp_func,
	emp_home_regn,
	operatng_ledger_buc,
	operatng_ledger_cc_cd,
	operatng_ledger_compny_cd,
	operatng_ledger_pl_cd,
	office_address,
	band_grp,
	supervisor_id_lvl_01,
	supervisor_name_lvl_01,
	supervisor_id_lvl_02,
	supervisor_name_lvl_02,
	supervisor_id_lvl_03,
	supervisor_name_lvl_03,
	supervisor_id_lvl_04,
	supervisor_name_lvl_04,
	supervisor_id_lvl_05,
	supervisor_name_lvl_05,
	supervisor_id_lvl_06,
	supervisor_name_lvl_06,
	supervisor_id_lvl_07,
	supervisor_name_lvl_07,
	supervisor_id_lvl_08,
	supervisor_name_lvl_08,
	supervisor_id_lvl_09,
	supervisor_name_lvl_09,
	supervisor_id_lvl_10,
	supervisor_name_lvl_10
)
SELECT 
	 temp.emp_id
	,temp.base_reportng_cd
	,temp.issuing_cntry_name
	,temp.traveler_name
	,temp.airline_alliance
	,temp.validatng_airline
	,temp.supplier_name
	,temp.travel_type_desc
	,temp.prod_type_desc
	,temp.eticket_flag
	,temp.obt_action_cd
	,temp.obt_vendor_cd
	,temp.agent_assist_rc
	,temp.tickets_used
	,temp.paid_fare_amt
	,temp.ref_fare_amt
	,temp.realized_savings_amt
	,temp.realized_savings_percent
	,temp.lowest_fare_amt
	,temp.missed_savings_amt
	,temp.missed_saving_percent
	,temp.potential_saivings_amt
	,temp.potential_savings_percent
	,temp.economy_compare_fare
	,temp.rc_missed_local_cd
	,temp.rc_missed_local_desc
	,temp.rc_realized_local_cd
	,temp.rc_realized_local_desc
	,temp.rc_missed_global_cd
	,temp.rc_missed_global_desc
	,temp.rc_realized_global_cd
	,temp.rc_realized_global_desc
	,temp.base_fare_amt
	,temp.tax_amt
	,temp.hst_tax_amt
	,temp.gst_tax_amt
	,temp.qst_tax_amt
	,temp.other_tax_amt
	,temp.pos_fee_amt
	,temp.main_airline_cd
	,temp.service_class_cd
	,temp.service_class_desc
	,temp.ticket_class_cd
	,temp.ticket_class_desc
	,temp.ticket_predominant_class
	,temp.fare_basis_cd
	,temp.air_booking_cd
	,temp.tour_cd
	,temp.ticket_designator
	,temp.ticket_nbr
	,temp.booking_dt
	,temp.invoice_nbr
	,temp.issue_dt
	,temp.ticket_depart_dt
	,temp.ticket_return_dt
	,temp.trip_days
	,temp.advance_purchase
	,temp.advance_booking
	,temp.ticket_routing
	,temp.od_airport_pair
	,temp.origin_airport_cd
	,temp.origin_city_name
	,temp.origin_cntry_name
	,temp.origin_regn_name
	,temp.destn_airport_cd
	,temp.destn_city_name
	,temp.dest_cntry_name
	,temp.dest_regn_name
	,temp.routing_type_desc
	,temp.nbr_of_sub_trips
	,temp.trip_miles
	,temp.sub_trip_miles
	,temp.flight_duration_min
	,temp.total_travel_time
	,temp.trip_type
	,temp.txn_type
	,temp.ticket_status_cd
	,temp.days_to_refund
	,temp.branch_id
	,temp.agent_id
	,temp.booking_iata_nbr
	,temp.ticketing_iata_nbr
	,temp.back_office_txn_id
	,temp.form_of_payment_cd
	,temp.credit_card_cd
	,temp.credit_card_nbr
	,temp.pnr
	,temp.gds
	,temp.travel_purpose
	,temp.accnt_nbr
	,temp.hier_1
	,temp.hier_2
	,temp.hier_3
	,temp.hier_4
	,temp.hier_5
	,temp.client_defined_1
	,temp.client_defined_2
	,temp.client_defined_3
	,temp.client_defined_4
	,temp.client_defined_5
	,temp.client_defined_6
	,temp.client_defined_7
	,temp.client_defined_8
	,temp.client_defined_9
	,temp.client_defined_10
	,temp.client_defined_11
	,temp.client_defined_12
	,temp.client_defined_13
	,temp.client_defined_14
	,temp.client_defined_15
	,temp.client_defined_16
	,temp.client_defined_17
	,temp.client_defined_18
	,temp.client_defined_19
	,temp.client_defined_20
	,temp.local_curncy_cd
	,temp.local_paid_fare
	,temp.co2e_kg
	,temp.kilometers
	,temp.main_airline_name
	,temp.tickets_issued
	,temp.txn_cnt
	,temp.client_master_cd_name
	,temp.booking_method_desc
	,temp.nbr_of_coupons
	,temp.org_id
	,temp.org_name
	,temp.orig_issue_dt
	,temp.orig_paid_fare
	,temp.orig_ticket_nbr
	,temp.orig_state_prov_cd
	,temp.dest_state_prov_cd
	,temp.src_last_upd_dt
	,temp.delete_record
	,temp.src_system_cd
	,temp.od_trip_type
	,temp.src_system_id
	,temp.src_name
	,'fin_tne_mtd_etl_scripts~fin_cwt_fact'
	,'fin_tne_mtd_etl_scripts~fin_cwt_fact'
	,current_timestamp
	,current_timestamp 
	,emp.business_segment AS business_segment,
	emp.sub_business as sub_business,
	emp.assignment_attrib_10 AS emp_org,
	emp.sso_id as SSO_ID,
	emp.workday_user_name,
	emp.emp_full_name,
	emp.email_address as email_address,
	emp.cntry_cd AS emp_cntry,
	emp.job_family_grp as job_family_grp,
	emp.job_family,
	emp.job_name,
	emp.business_grp_name,
	emp.emp_func,
	emp.emp_home_regn as emp_home_regn,
	emp.operatng_ledger_buc,
	emp.operatng_ledger_cc_cd as operatng_ledger_cc_cd,
	emp.operatng_ledger_compny_cd as operatng_ledger_compny_cd,
	emp.operatng_ledger_pl_cd,
	emp.office_address,
	emp.band_grp,
	emp.supervisor_id_lvl_01,
	emp.supervisor_name_lvl_01,
	emp.supervisor_id_lvl_02,
	emp.supervisor_name_lvl_02,
	emp.supervisor_id_lvl_03,
	emp.supervisor_name_lvl_03,
	emp.supervisor_id_lvl_04,
	emp.supervisor_name_lvl_04,
	emp.supervisor_id_lvl_05,
	emp.supervisor_name_lvl_05,
	emp.supervisor_id_lvl_06,
	emp.supervisor_name_lvl_06,
	emp.supervisor_id_lvl_07,
	emp.supervisor_name_lvl_07,
	emp.supervisor_id_lvl_08,
	emp.supervisor_name_lvl_08,
	emp.supervisor_id_lvl_09,
	emp.supervisor_name_lvl_09,
	emp.supervisor_id_lvl_10,
	emp.supervisor_name_lvl_10
FROM
TEMP_AIR_RESERVATIONS temp
LEFT JOIN al_finance.fin_employee_d emp ON temp.emp_id = emp.src_system_id
LEFT JOIN AL_FINANCE.FIN_TNE_AIR_RESERVATION_F f ON temp.src_system_id = f.src_system_id
WHERE
f.src_system_id is null;