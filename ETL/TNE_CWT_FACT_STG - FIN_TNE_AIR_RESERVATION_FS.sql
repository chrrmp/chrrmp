--STEP 1

TRUNCATE TABLE AL_FINANCE.FIN_TNE_AIR_RESERVATION_FS;
--STEP 2

INSERT into AL_FINANCE.FIN_TNE_AIR_RESERVATION_FS
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
	,dl_upd_ts)
SELECT 
 a.client_defined_field_6
,a.base_reporting_code
,a.issuing_country_name
,a.traveler_name
,a.airline_alliance
,a.validating_airline
,a.supplier_name
,a.travel_type
,a.product_type_description
,a.e_ticket
,a.obt_action_code
,a.obt_vendor_code
,a.agent_assist_rc
,a.tickets_used
,CAST(a.paid_fare AS NUMERIC)
,CAST(a.reference_fare AS NUMERIC)
,CAST(a.realized_savings AS NUMERIC)
,CAST(a.perc_realized_savings AS NUMERIC)
,CAST(a.lowest_fare AS NUMERIC)
,CAST(a.missed_savings AS NUMERIC)
,CAST(a.perc_missed_savings AS NUMERIC)
,CAST(a.potential_savings AS NUMERIC)
,CAST(a.perc_potential_savings AS NUMERIC)
,CAST(a.economy_comparision_fare AS NUMERIC)
,a.rc_missed_local
,a.rc_missed_local_description
,a.rc_realized_local
,a.rc_realized_local_description
,a.rc_missed_global
,a.rc_missed_global_description
,a.rc_realized_global
,a.rc_realized_global_description
,CAST(a.base_fare AS NUMERIC)
,CAST(a.tax AS NUMERIC)
,CAST(a.hst_tax AS NUMERIC)
,CAST(a.gst_tax AS NUMERIC)
,CAST(a.qst_tax AS NUMERIC)
,CAST(a.other_tax AS NUMERIC)
,CAST(a.point_of_sale_fee AS NUMERIC)
,a.main_airline
,a.service_class_code
,a.service_class
,a.ticket_class
,a.ticket_class_description
,a.predominant_ticket_class
,a.fare_basis_code
,a.air_booking_code
,a.tour_code
,a.ticket_designator
,a.ticket_number
,TO_DATE(a.BOOKING_DATE, 'DD-MON-YYYY') 
,a.invoice_number
,TO_DATE(a.issue_date, 'DD-MON-YYYY') 
,TO_DATE(a.ticket_departure_date, 'DD-MON-YYYY') 
,TO_DATE(a.ticket_return_date, 'DD-MON-YYYY')  
,a.trip_days
,a.advance_ticketing
,a.advance_booking
,a.ticket_routing
,a.o_d_airport_pair
,a.origin_airport_code
,a.origin_city_name
,a.origin_country
,a.origin_region
,a.destination_airport_code
,a.destination_city_name
,a.destination_country
,a.destination_region
,a.routing_type
,a.number_of_sub_trips
,a.trip_miles
,a.sub_trip_miles
,a.flight_duration
,a.total_travel_time
,a.trip_type
,a.transaction_type
,a.ticket_status
,a.days_to_refund
,a.branch_id
,a.agent_id
,a.booking_iata_number
,a.ticketing_iata_number
,a.backoffice_tran_id
,a.form_of_payment
,a.credit_card_code
,a.credit_card_number
,a.pnr
,a.gds
,a.travel_purpose
,a.account_number
,a.hierarchy_1
,a.hierarchy_2
,a.hierarchy_3
,a.hierarchy_4
,a.hierarchy_5
,a.client_defined_field_1
,a.client_defined_field_2
,a.client_defined_field_3
,a.client_defined_field_4
,a.client_defined_field_5
,a.client_defined_field_6
,a.client_defined_field_7
,a.client_defined_field_8
,a.client_defined_field_9
,a.client_defined_field_10
,a.client_defined_field_11
,a.client_defined_field_12
,a.client_defined_field_13
,a.client_defined_field_14
,a.client_defined_field_15
,a.client_defined_field_16
,a.client_defined_field_17
,a.client_defined_field_18
,a.client_defined_field_19
,a.client_defined_field_20
,a.local_currency_code
,CAST(local_paid_fare AS NUMERIC) 
,CAST(co2e_kg AS NUMERIC) 
,CAST(kilometers AS NUMERIC)
,a.main_airline_name
,CAST(a.ticket_issued AS NUMERIC)
,CAST(a.transaction_count AS NUMERIC)
,a.base_reporting_name
,a.booking_method
,CAST(a.number_of_coupons AS NUMERIC)
,a.organization_id
,a.organization
,TO_DATE(a.original_issue_date, 'DD-MON-YYYY')  
,CAST(A.original_paid_fare AS NUMERIC)
,a.original_ticket_number
,a.origin_state_province_code
,a.destination_state_province_code
,TO_DATE(a.last_update_date, 'MM/DD/YYYY') 
,CAST(a.delete_record AS NUMERIC)
,a.source_system_code
,a.od_trip_type
,(a.backoffice_tran_id||'~'||'CWT') 
,'CWT' 
, 'fin_tne_mtd_etl_scripts~tne_cwt_fact_stg' 
,'fin_tne_mtd_etl_scripts~tne_cwt_fact_stg' 
,current_timestamp  
,current_timestamp 
FROM 
OG_FILE_SRC.FIN_TNE_AIR_RESERVATION A ;
