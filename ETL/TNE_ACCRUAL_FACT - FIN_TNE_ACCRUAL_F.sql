--STEP 1

DELETE FROM al_finance.fin_tne_accrual_f where snapshot_dt = current_date;
--STEP 2

INSERT INTO al_finance.fin_tne_accrual_f 
(
	snapshot_id
	,snapshot_dt
	,emp_id
	,dtl_format_indicatr
	,accrual_serial_nbr
	,concur_submission_id
	,last_name
	,first_name
	,middle_name
	,emp_tax_nbr
	,cap_user_id
	,cap_user_nbr
	,cap_user_ap_nbr
	,ohr_cost_center
	,payment_type
	,payment_type_ap_nbr
	,ohr_business_grp
	,card_txn_type
	,custom3_segment_1
	,txn_type
	,txn_nbr
	,vendor_name
	,vendor_mcc_cd
	,payment_method
	,payment_type_accnt_nbr
	,payment_accnt_nbr
	,internatnl_dom_flag
	,etran_iso_cntry_code
	,entity_iso_cntry_code
	,entity_iso_curncy_code
	,submission_name
	,txn_amt
	,txn_curncy
	,custom_flexfield_3
	,submit_dt
	,txn_dt
	,journal_amt
	,debit_credit_flag
	,ohr_adn
	,char_flexfield_2
	,ohr_buc
	,ohr_dept
	,ohr_labor_cd
	,ohr_local_band
	,cc_txn_to_emp_flag
	,flexfield_int_2
	,flexfield_int_3
	,flexfield_float_1
	,flexfield_float_2
	,flexfield_float_3
	,posting_dt
	,flexfield_dt_2
	,flexfield_dt_3
	,filler
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
	ROW_NUMBER() OVER() AS SNAPSHOT_ID
	,CURRENT_DATE AS SNAPSHOT_DT
	,COALESCE(emp.src_system_id,fs.cap_user_nbr) AS EMP_ID
	,fs.dtl_format_indicatr
	,fs.accrual_serial_nbr
	,fs.concur_submission_id
	,fs.last_name
	,fs.first_name
	,fs.middle_name
	,fs.emp_tax_nbr
	,fs.cap_user_id
	,fs.cap_user_nbr
	,fs.cap_user_ap_nbr
	,fs.ohr_cost_center
	,fs.payment_type
	,fs.payment_type_ap_nbr
	,fs.ohr_business_grp
	,fs.card_txn_type
	,fs.custom3_segment_1
	,fs.txn_type
	,fs.txn_nbr
	,fs.vendor_name
	,fs.vendor_mcc_cd
	,fs.payment_method
	,fs.payment_type_accnt_nbr
	,fs.payment_accnt_nbr
	,fs.internatnl_dom_flag
	,fs.etran_iso_cntry_code
	,fs.entity_iso_cntry_code
	,fs.entity_iso_curncy_code
	,fs.submission_name
	,fs.txn_amt
	,fs.txn_curncy
	,fs.custom_flexfield_3
	,fs.submit_dt
	,fs.txn_dt
	,fs.journal_amt
	,fs.debit_credit_flag
	,fs.ohr_adn
	,fs.char_flexfield_2
	,fs.ohr_buc
	,fs.ohr_dept
	,fs.ohr_labor_cd
	,fs.ohr_local_band
	,fs.cc_txn_to_emp_flag
	,fs.flexfield_int_2
	,fs.flexfield_int_3
	,fs.flexfield_float_1
	,fs.flexfield_float_2
	,fs.flexfield_float_3
	,fs.posting_dt
	,fs.flexfield_dt_2
	,fs.flexfield_dt_3
	,fs.filler
	,fs.src_system_id
	,fs.src_name
	,'fin_tne_mtd_etl_scripts~tne_accrual_fact' as dl_ins_by
	,'fin_tne_mtd_etl_scripts~tne_accrual_fact' as dl_upd_by
	,CURRENT_TIMESTAMP as dl_ins_ts
	,CURRENT_TIMESTAMP as dl_upd_ts
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
al_finance.fin_tne_accrual_fs fs
left join (SELECT * from 
		(select *,row_number() over(partition by sso_id order by status desc) rn
		from al_finance.fin_employee_d) emp_d where emp_d.rn = 1) emp on fs.cap_user_nbr = emp.sso_id ;
