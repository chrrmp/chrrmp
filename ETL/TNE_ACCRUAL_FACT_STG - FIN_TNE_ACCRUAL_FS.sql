--STEP 1

TRUNCATE TABLE al_finance.fin_tne_accrual_fs ;
--STEP 2

INSERT INTO AL_FINANCE.FIN_TNE_ACCRUAL_FS
(
 dtl_format_indicatr
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
)
SELECT 
Detail_Format_indicator
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
,card_transaction_type
,custom3_segment_1
,transaction_type
,transaction_nbr
,vendor_name
,vendor_mcc_cd
,payment_method
,payment_type_accnt_nbr
,payment_accnt_nbr
,international_domestic_flag
,etran_iso_country_code
,entity_iso_country_code
,entiry_iso_currency_code
,submission_name
,CASE WHEN transaction_amount='' THEN NULL ELSE transaction_amount::NUMERIC END
,transaction_currency
,custom_flexfield_3
,CASE WHEN submit_date='' THEN NULL ELSE submit_date::TIMESTAMP END
,CASE WHEN transaction_date='' THEN NULL ELSE transaction_date::TIMESTAMP END
,CASE WHEN journal_amt='' THEN NULL ELSE journal_amt::NUMERIC END
,debit_credit_flag
,ohr_adn
,char_flexfield_2
,ohr_buc
,ohr_dept
,ohr_labor_cd
,ohr_local_band
,CASE WHEN cc_txn_to_emp_flag='' THEN NULL ELSE cc_txn_to_emp_flag::INTEGER END
,CASE WHEN flexfield_int_2='' THEN NULL ELSE flexfield_int_2::INTEGER END
,CASE WHEN flexfield_int_3='' THEN NULL ELSE flexfield_int_3::INTEGER END
,CASE WHEN flexfield_float_1='' THEN NULL ELSE flexfield_float_1::NUMERIC END
,CASE WHEN flexfield_float_2='' THEN NULL ELSE flexfield_float_2::NUMERIC END
,CASE WHEN flexfield_float_3='' THEN NULL ELSE flexfield_float_3::NUMERIC END
,CASE WHEN posting_date='' THEN NULL ELSE posting_date::TIMESTAMP END
,CASE WHEN flexfield_date_2='' THEN NULL ELSE flexfield_date_2::TIMESTAMP END
,CASE WHEN flexfield_date_3='' THEN NULL ELSE flexfield_date_3::TIMESTAMP END
,filler
,coalesce(cap_user_nbr,'') || '~' || coalesce(transaction_nbr,'') || '~' || row_number() over() as SRC_SYSTEM_ID
,'CON' AS src_name
,'fin_tne_mtd_etl_scripts~tne_accrual_fact_stg' as dl_ins_by
,'fin_tne_mtd_etl_scripts~tne_accrual_fact_stg' as dl_upd_by
,CURRENT_TIMESTAMP as dl_ins_ts
,CURRENT_TIMESTAMP as dl_upd_ts
from 
OG_FILE_SRC.FIN_TNE_ACCRUAL_EXTRACT;
