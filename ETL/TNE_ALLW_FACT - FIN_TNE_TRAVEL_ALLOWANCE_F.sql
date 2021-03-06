--STEP 1

INSERT INTO al_finance.fin_tne_travel_allowance_f (
payslip_code  ,
payslip_desc ,
payroll_year ,
payroll_month ,
legal_entity_id  ,
glid_company_code ,
emp_sso_id  ,
employee_id ,
trip_start_dt,
trip_end_dt,
business_travel_date  ,
approval_dt,
---transaction_dt ,
travel_allowance_payslip_amt  ,
travel_allowance_type ,
travel_allowance_ctgry,
taxable_code ,
expense_type ,
travel_country ,
travel_city ,
travel_type ,
report_name ,
expense_id,
emp_band,
report_dt,
report_key,
report_id,
report_entry_id,
trvl_allowance_amt,
trvl_allowance_amt_forfeit,
trvl_allowance_amt_holiday,
src_system_id  ,
src_name  ,
dl_ins_by  ,
dl_upd_by  ,
dl_ins_ts  ,
dl_upd_ts  )
SELECT
payslip.emp_payslip_item_code,
payslip.description,
EXTRACT(YEAR FROM expense.approval_dt),
EXTRACT(MONTH FROM expense.approval_dt),
expense.legal_entity,
expense.glid_company_cd,
expense.sso_id,
expense.employee_id,
expense.trip_start_dt,
expense.trip_end_dt,
expense.transaction_dt,
expense.approval_dt,
--expense.transaction_dt,
round(expense.forfeit_taxable_amt,2),
expense.trvl_allowance_type,
expense.travel_allowance_ctgry,
'Taxable',
expense.expense_type,
expense.country_desc,
expense.city,
expense.trvl_type,
expense.report_name,
expense.src_system_id,
expense.emp_band,
expense.report_dt,
expense.report_key,
expense.report_id,
expense.report_entry_id,
round(expense.trvl_allowance_amt,2),
round(expense.trvl_allowance_amt_forfeit,2),
round(expense.trvl_allowance_amt_holiday,2),
payslip.emp_payslip_item_code || '~' || expense.src_system_id,
expense.src_name,
'fin_tne_mtd_etl_scripts~tne_allw_fact'  ,
'fin_tne_mtd_etl_scripts~tne_allw_fact'  ,
CURRENT_TIMESTAMP,
CURRENT_TIMESTAMP
FROM al_finance.fin_tne_travel_allowance_fs expense
INNER JOIN (SELECT emp_payslip_item_code,description from 
				al_finance.fin_tne_ref_payslip_code where taxable_cd='Taxed' AND trvl_allowance_cat = 'Mixed Forfeit') payslip ON 1=1
WHERE expense.forfeit_taxable_amt > 0;
--STEP 2

INSERT INTO al_finance.fin_tne_travel_allowance_f (
payslip_code  ,
payslip_desc ,
payroll_year ,
payroll_month ,
legal_entity_id  ,
glid_company_code ,
emp_sso_id  ,
employee_id ,
trip_start_dt,
trip_end_dt,
business_travel_date  ,
approval_dt,
---transaction_dt ,
travel_allowance_payslip_amt  ,
travel_allowance_type ,
travel_allowance_ctgry,
taxable_code ,
expense_type ,
travel_country ,
travel_city ,
travel_type ,
report_name ,
expense_id,
emp_band,
report_dt,
report_key,
report_id,
report_entry_id,
trvl_allowance_amt,
trvl_allowance_amt_forfeit,
trvl_allowance_amt_holiday,
src_system_id  ,
src_name  ,
dl_ins_by  ,
dl_upd_by  ,
dl_ins_ts  ,
dl_upd_ts  )
SELECT
payslip.emp_payslip_item_code,
payslip.description,
EXTRACT(YEAR FROM expense.approval_dt),
EXTRACT(MONTH FROM expense.approval_dt),
expense.legal_entity,
expense.glid_company_cd,
expense.sso_id,
expense.employee_id,
expense.trip_start_dt,
expense.trip_end_dt,
expense.transaction_dt,
expense.approval_dt,
--expense.transaction_dt,
round(expense.forfeit_non_taxable_amt,2),
expense.trvl_allowance_type,
expense.travel_allowance_ctgry,
'Taxable',
expense.expense_type,
expense.country_desc,
expense.city,
expense.trvl_type,
expense.report_name,
expense.src_system_id,
expense.emp_band,
expense.report_dt,
expense.report_key,
expense.report_id,
expense.report_entry_id,
round(expense.trvl_allowance_amt,2),
round(expense.trvl_allowance_amt_forfeit,2),
round(expense.trvl_allowance_amt_holiday,2),
payslip.emp_payslip_item_code || '~' || expense.src_system_id,
expense.src_name,
'fin_tne_mtd_etl_scripts~tne_allw_fact'  ,
'fin_tne_mtd_etl_scripts~tne_allw_fact'  ,
CURRENT_TIMESTAMP,
CURRENT_TIMESTAMP
FROM al_finance.fin_tne_travel_allowance_fs expense
INNER JOIN (SELECT emp_payslip_item_code,description from 
				al_finance.fin_tne_ref_payslip_code where taxable_cd='Non-Taxed' AND trvl_allowance_cat = 'Mixed Forfeit') payslip ON 1=1
WHERE expense.forfeit_non_taxable_amt > 0;
--STEP 3

INSERT INTO al_finance.fin_tne_travel_allowance_f (
payslip_code  ,
payslip_desc ,
payroll_year ,
payroll_month ,
legal_entity_id  ,
glid_company_code ,
emp_sso_id  ,
employee_id ,
trip_start_dt,
trip_end_dt,
business_travel_date  ,
approval_dt,
---transaction_dt ,
travel_allowance_payslip_amt  ,
travel_allowance_type ,
travel_allowance_ctgry,
taxable_code ,
expense_type ,
travel_country ,
travel_city ,
travel_type ,
report_name ,
expense_id,
emp_band,
report_dt,
report_key,
report_id,
report_entry_id,
trvl_allowance_amt,
trvl_allowance_amt_forfeit,
trvl_allowance_amt_holiday,
src_system_id  ,
src_name  ,
dl_ins_by  ,
dl_upd_by  ,
dl_ins_ts  ,
dl_upd_ts  )
SELECT
payslip.emp_payslip_item_code,
payslip.description,
EXTRACT(YEAR FROM expense.approval_dt),
EXTRACT(MONTH FROM expense.approval_dt),
expense.legal_entity,
expense.glid_company_cd,
expense.sso_id,
expense.employee_id,
expense.trip_start_dt,
expense.trip_end_dt,
expense.transaction_dt,
expense.approval_dt,
--expense.transaction_dt,
round(expense.trvl_allowance_amt_tax,2),
expense.trvl_allowance_type,
expense.travel_allowance_ctgry,
'Taxable',
expense.expense_type,
expense.country_desc,
expense.city,
expense.trvl_type,
expense.report_name,
expense.src_system_id,
expense.emp_band,
expense.report_dt,
expense.report_key,
expense.report_id,
expense.report_entry_id,
round(expense.trvl_allowance_amt,2),
round(expense.trvl_allowance_amt_forfeit,2),
round(expense.trvl_allowance_amt_holiday,2),
payslip.emp_payslip_item_code || '~' || expense.src_system_id,
expense.src_name,
'fin_tne_mtd_etl_scripts~tne_allw_fact'  ,
'fin_tne_mtd_etl_scripts~tne_allw_fact'  ,
CURRENT_TIMESTAMP,
CURRENT_TIMESTAMP
FROM al_finance.fin_tne_travel_allowance_fs expense
INNER JOIN (SELECT emp_payslip_item_code,description from 
				al_finance.fin_tne_ref_payslip_code where taxable_cd='Taxed' AND trvl_allowance_cat = 'Receipt Based') payslip ON 1=1
WHERE expense.trvl_allowance_amt_tax > 0;
--STEP 4

INSERT INTO al_finance.fin_tne_travel_allowance_f (
payslip_code  ,
payslip_desc ,
payroll_year ,
payroll_month ,
legal_entity_id  ,
glid_company_code ,
emp_sso_id  ,
employee_id ,
trip_start_dt,
trip_end_dt,
business_travel_date  ,
approval_dt,
---transaction_dt ,
travel_allowance_payslip_amt  ,
travel_allowance_type ,
travel_allowance_ctgry,
taxable_code ,
expense_type ,
travel_country ,
travel_city ,
travel_type ,
report_name ,
expense_id,
emp_band,
report_dt,
report_key,
report_id,
report_entry_id,
trvl_allowance_amt,
trvl_allowance_amt_forfeit,
trvl_allowance_amt_holiday,
src_system_id  ,
src_name  ,
dl_ins_by  ,
dl_upd_by  ,
dl_ins_ts  ,
dl_upd_ts  )
SELECT
payslip.emp_payslip_item_code,
payslip.description,
EXTRACT(YEAR FROM expense.approval_dt),
EXTRACT(MONTH FROM expense.approval_dt),
expense.legal_entity,
expense.glid_company_cd,
expense.sso_id,
expense.employee_id,
expense.trip_start_dt,
expense.trip_end_dt,
expense.transaction_dt,
expense.approval_dt,
--expense.transaction_dt,
round(expense.trvl_allowance_amt_non_tax,2),
expense.trvl_allowance_type,
expense.travel_allowance_ctgry,
'Non-Taxable',
expense.expense_type,
expense.country_desc,
expense.city,
expense.trvl_type,
expense.report_name,
expense.src_system_id,
expense.emp_band,
expense.report_dt,
expense.report_key,
expense.report_id,
expense.report_entry_id,
round(expense.trvl_allowance_amt,2),
round(expense.trvl_allowance_amt_forfeit,2),
round(expense.trvl_allowance_amt_holiday,2),
payslip.emp_payslip_item_code || '~' || expense.src_system_id,
expense.src_name,
'fin_tne_mtd_etl_scripts~tne_allw_fact'  ,
'fin_tne_mtd_etl_scripts~tne_allw_fact'  ,
CURRENT_TIMESTAMP,
CURRENT_TIMESTAMP
FROM al_finance.fin_tne_travel_allowance_fs expense
INNER JOIN (SELECT emp_payslip_item_code,description from 
				al_finance.fin_tne_ref_payslip_code where taxable_cd='Non-Taxed' AND trvl_allowance_cat = 'Receipt Based') payslip ON 1=1
WHERE expense.trvl_allowance_amt_non_tax > 0;
--STEP 5

INSERT INTO al_finance.fin_tne_travel_allowance_f (
payslip_code  ,
payslip_desc ,
payroll_year ,
payroll_month ,
legal_entity_id  ,
glid_company_code ,
emp_sso_id  ,
employee_id ,
trip_start_dt,
trip_end_dt,
business_travel_date  ,
approval_dt,
---transaction_dt ,
travel_allowance_payslip_amt  ,
travel_allowance_type ,
travel_allowance_ctgry,
taxable_code ,
expense_type ,
travel_country ,
travel_city ,
travel_type ,
report_name ,
expense_id,
emp_band,
report_dt,
report_key,
report_id,
report_entry_id,
trvl_allowance_amt,
trvl_allowance_amt_forfeit,
trvl_allowance_amt_holiday,
src_system_id  ,
src_name  ,
dl_ins_by  ,
dl_upd_by  ,
dl_ins_ts  ,
dl_upd_ts  )
SELECT
payslip.emp_payslip_item_code,
payslip.description,
EXTRACT(YEAR FROM expense.approval_dt),
EXTRACT(MONTH FROM expense.approval_dt),
expense.legal_entity,
expense.glid_company_cd,
expense.sso_id,
expense.employee_id,
expense.trip_start_dt,
expense.trip_end_dt,
expense.transaction_dt,
expense.approval_dt,
--expense.transaction_dt,
round(expense.holiday_taxable_amt,2),
expense.trvl_allowance_type,
'Holiday Allowance',
'Taxable',
expense.expense_type,
expense.country_desc,
expense.city,
expense.trvl_type,
expense.report_name,
expense.src_system_id,
expense.emp_band,
expense.report_dt,
expense.report_key,
expense.report_id,
expense.report_entry_id,
round(expense.trvl_allowance_amt,2),
round(expense.trvl_allowance_amt_forfeit,2),
round(expense.trvl_allowance_amt_holiday,2),
payslip.emp_payslip_item_code || '~' || expense.src_system_id,
expense.src_name,
'fin_tne_mtd_etl_scripts~tne_allw_fact',
'fin_tne_mtd_etl_scripts~tne_allw_fact',
current_timestamp,
current_timestamp
FROM (SELECT * from al_finance.fin_tne_travel_allowance_fs WHERE holiday_flg='Y') expense
INNER JOIN (SELECT emp_payslip_item_code,description FROM al_finance.fin_tne_ref_payslip_code 
					WHERE taxable_cd='Taxed' and trvl_allowance_cat = 'Holiday Allowance') payslip ON 1=1
WHERE expense.holiday_taxable_amt > 0;
--STEP 6

INSERT INTO al_finance.fin_tne_travel_allowance_f (
payslip_code  ,
payslip_desc ,
payroll_year ,
payroll_month ,
legal_entity_id  ,
glid_company_code ,
emp_sso_id  ,
employee_id ,
trip_start_dt,
trip_end_dt,
business_travel_date  ,
approval_dt,
---transaction_dt ,
travel_allowance_payslip_amt  ,
travel_allowance_type ,
travel_allowance_ctgry,
taxable_code ,
expense_type ,
travel_country ,
travel_city ,
travel_type ,
report_name ,
expense_id,
emp_band,
report_dt,
report_key,
report_id,
report_entry_id,
trvl_allowance_amt,
trvl_allowance_amt_forfeit,
trvl_allowance_amt_holiday,
src_system_id  ,
src_name  ,
dl_ins_by  ,
dl_upd_by  ,
dl_ins_ts  ,
dl_upd_ts  )
SELECT
payslip.emp_payslip_item_code,
payslip.description,
EXTRACT(YEAR FROM expense.approval_dt),
EXTRACT(MONTH FROM expense.approval_dt),
expense.legal_entity,
expense.glid_company_cd,
expense.sso_id,
expense.employee_id,
expense.trip_start_dt,
expense.trip_end_dt,
expense.transaction_dt,
expense.approval_dt,
--expense.transaction_dt,
round(expense.holiday_non_taxable_amt,2),
expense.trvl_allowance_type,
'Holiday Allowance',
'Non-Taxable',
expense.expense_type,
expense.country_desc,
expense.city,
expense.trvl_type,
expense.report_name,
expense.src_system_id,
expense.emp_band,
expense.report_dt,
expense.report_key,
expense.report_id,
expense.report_entry_id,
round(expense.trvl_allowance_amt,2),
round(expense.trvl_allowance_amt_forfeit,2),
round(expense.trvl_allowance_amt_holiday,2),
payslip.emp_payslip_item_code || '~' || expense.src_system_id,
expense.src_name,
'fin_tne_mtd_etl_scripts~tne_allw_fact',
'fin_tne_mtd_etl_scripts~tne_allw_fact',
current_timestamp,
current_timestamp
FROM (SELECT * from al_finance.fin_tne_travel_allowance_fs WHERE holiday_flg='Y') expense
INNER JOIN (SELECT emp_payslip_item_code,description FROM al_finance.fin_tne_ref_payslip_code 
					WHERE taxable_cd='Non-Taxed' and trvl_allowance_cat = 'Holiday Allowance') payslip ON 1=1
WHERE  expense.holiday_non_taxable_amt > 0;
