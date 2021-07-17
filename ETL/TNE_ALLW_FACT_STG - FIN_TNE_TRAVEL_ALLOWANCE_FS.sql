--STEP 1

TRUNCATE table al_finance.fin_tne_travel_allowance_fs;
--STEP 2

CREATE TEMP TABLE temp1_fin_tne_travel_allowance_fs
AS (
SELECT
expense.emp_custom_sso_id AS SSO_ID,
expense.emp_id AS employee_id,
legalent.glid AS glid_company_cd,
expense.rep_custom_18 AS trvl_allowance_type,
expense.rep_paymnt_processing_dt AS approval_dt,
expense.rep_entry_expense_type_name AS expense_type,
rep_entry_loctn_cntry_cd AS country_cd,
country.cntry_desc AS country_desc,
expense.rep_entry_locatn_city_name AS city,
expense.rep_entry_foreign_domes_flg AS travel_flag,
expense.rep_entry_txn_dt AS transaction_dt,
employee.local_band AS emp_band,
allowancecat.trvl_allowance_category AS travel_allowance_ctgry,
CASE WHEN expense.rep_entry_foreign_domes_flg='HOME' THEN 'Domestic'
     WHEN expense.rep_entry_foreign_domes_flg='FRGN' THEN 'International'
END AS TRVL_TYPE,
legalent.legal_entity_code AS LEGAL_ENTITY,
expense.rep_name AS REPORT_NAME,
expense.rep_submit_dt REPORT_DT,
expense.rep_key AS REPORT_KEY,
expense.rep_id AS REPORT_ID,
expense.rep_entry_id AS REPORT_ENTRY_ID,
--MIN(expense.rep_entry_txn_dt) over(partition by expense.emp_custom_sso_id,expense.rep_key) as trip_start_dt,  --- Trip_start_dt
--MAX(expense.rep_entry_txn_dt) over(partition by expense.emp_custom_sso_id,expense.rep_key) as trip_end_dt,  --- Trip_end_dt
trip_it.trip_start_dt AS trip_start_dt,      --- enable this once the new itinerary file from concur/cognos is integrated
trip_it.trip_end_dt AS trip_end_dt,            --- enable this once the new itinerary file from concur/cognos is integrated
case when expense.rep_entry_foreign_domes_flg = 'HOME' then coalesce(domestic_allowance.TRVL_ALLOWANCE,0) else 0 end as home_trvl_allowance_amt,
expense.src_system_id AS SRC_SYSTEM_ID,
expense.src_name AS SRC_NAME,
CURRENT_TIMESTAMP AS dl_ins_ts,
CURRENT_TIMESTAMP AS dl_upd_ts,
CASE WHEN expense.rep_entry_foreign_domes_flg='FRGN' then (COALESCE(f_traveldays.cur_traveldays,0) + row_number() 
			over(partition by expense.emp_custom_sso_id,extract(year from expense.rep_entry_txn_dt) order by expense.rep_entry_txn_dt))
				ELSE 0 END as travel_days_counter
FROM
---------------------Extract Incremental Data from expense Fact
	(
	SELECT
	expense_src.* 
	FROM al_finance.fin_tne_expense_f expense_src
	LEFT JOIN al_finance.fin_tne_travel_allowance_f allw ON expense_src.src_system_id=allw.expense_id   
	WHERE 
	expense_src.rep_entry_expense_type_name='Daily Allowance' AND allw.expense_id IS NULL
	) expense
----------------------- To restrict data for the 4 GLIDs
		INNER JOIN al_finance.fin_tne_ref_legal_entity legalent ON expense.emp_org_unit_glid_compny_cd=legalent.glid
----------------------- To restrict data for the 6 Travel Allowance Types / may be codes
		INNER JOIN al_finance.fin_tne_ref_allowance_category allowancecat ON expense.rep_custom_18=allowancecat.trvl_allowance_type
---------------------- To derive Trip Start date and Trip End date
		INNER JOIN al_finance.fin_tne_itinerary_ref_d trip_it ON trip_it.report_id = expense.rep_id 
										AND expense.rep_entry_txn_dt BETWEEN trip_it.trip_start_dt and trip_it.trip_end_dt
---------------------- To derive the Employee Local Band
		INNER JOIN al_finance.fin_employee_d employee ON expense.emp_custom_sso_id=employee.sso_id
---------------------- To derive country description
		INNER JOIN al_finance.fin_cntry_hier_d country ON expense.rep_entry_loctn_cntry_cd=country.cntry_cd
----------------------- To derive domestic allowance (Travel Type = 'HOME')
		LEFT JOIN al_finance.fin_tne_ref_domestic_allowance_rate domestic_allowance on employee.local_band = domestic_allowance.emp_level
----------------------  To derive the number of travel days for international travel
		LEFT JOIN (SELECT Fact.SSO_ID as SSO_ID,Fact.transaction_year as transaction_year,
									coalesce(fact.cur_days,0) + coalesce(emp_his_days.intl_travel_days,0) AS cur_traveldays
						FROM
							(SELECT emp_sso_id as SSO_ID,extract(year from business_travel_date) as transaction_year,
										count(distinct business_travel_date) as cur_days
							   from al_finance.fin_tne_travel_allowance_f
							  where travel_type='International'
							   group by emp_sso_id,extract(year from business_travel_date)) Fact 
									left join al_finance.fin_tne_emp_travel_days_d emp_his_days 
											ON emp_his_days.emp_id = fact.SSO_ID and emp_his_days.year = fact.transaction_year ) f_traveldays
							ON expense.emp_custom_sso_id=f_traveldays.SSO_ID and extract(year from expense.rep_entry_txn_dt) = f_traveldays.transaction_year
) DISTRIBUTED BY (SRC_SYSTEM_ID);
--STEP 3

analyze temp1_fin_tne_travel_allowance_fs;
--STEP 4

CREATE TEMP TABLE temp2_fin_tne_travel_allowance_fs
AS
(
SELECT 
SRC.*,
(COALESCE(SRC.home_trvl_allowance_amt,0) + COALESCE(SRC.global_trvl_allowance_amt,0)) AS trvl_allowance_amt,
COALESCE(SRC.forfeit_allowance_eur_amt,0) AS forfeit_trvl_allowance_amt,
(COALESCE(SRC.pubhol_startweek_allowance_amt,0) + COALESCE(SRC.pubhol_endweek_allowance_amt,0)) AS holiday_trvl_allowance_amt
FROM 
(
SELECT
expense.sso_id,
expense.employee_id,
expense.glid_company_cd,
expense.approval_dt,
expense.expense_type,
expense.country_cd,
expense.country_desc,
expense.city,
expense.transaction_dt,
expense.legal_entity,
expense.report_name,
expense.report_dt,
expense.report_key,
expense.report_id,
expense.report_entry_id,
expense.trip_start_dt,  --- step 3
expense.trip_end_dt,  --- new --- trip_end_dt
expense.src_system_id,
expense.src_name,
expense.dl_ins_ts,
expense.dl_upd_ts,
expense.trvl_allowance_type,
expense.trvl_type,
expense.travel_flag,
expense.travel_allowance_ctgry,
expense.emp_band,
expense.travel_days_counter,
-----------Non Taxable Amount Limit
non_tax_limit.non_taxable_limit,
-----------Home Travel Allowance Amount
expense.home_trvl_allowance_amt,  --- step 4
----------Global Travel Allowance Amount
COALESCE(CASE 
	WHEN COALESCE(destination_city.country_severity_lvl,destination_all.country_severity_lvl)='Level 1' 
					THEN COALESCE(trvlmin_globalallowance.dest_sev1_allowance,trvlmax_globalallowance.dest_sev1_allowance )
	WHEN COALESCE(destination_city.country_severity_lvl,destination_all.country_severity_lvl)='Level 2' 
					THEN COALESCE(trvlmin_globalallowance.dest_sev2_allowance,trvlmax_globalallowance.dest_sev2_allowance )
															ELSE 0  END,0) AS global_trvl_allowance_amt  ,                    ----- Step 5 & 6
----------Forfiet Amount
COALESCE(CASE WHEN expense.travel_allowance_ctgry='Mixed Forfeit' THEN COALESCE(forfeit_ref.amount,forfeit_all.amount) 
					ELSE 0 END,0) as forfeit_allowance_amt,            -------- STEP 7
----------Forfiet Amount in EURO
COALESCE(CASE WHEN expense.travel_allowance_ctgry='Mixed Forfeit' 
					THEN ROUND((COALESCE(forfeit_ref.amount,forfeit_all.amount) * mor.mor_rate) * usdtoeuro.usd_to_eur_rt,2) 
					ELSE 0 END,0) AS forfeit_allowance_eur_amt  ,                            -------- STEP 7
----------Holiday allowance for trip start date = Public holiday
--COALESCE(CASE WHEN pubhol_start.date is not null then holidayref.allowance_amt else 0 end,0) AS pub_holi_start_allowance_amt ,    ---- Step 8
----------Holiday allowance for trip end date = Public holiday
--COALESCE(CASE WHEN pubhol_end.date is not null then holidayref.allowance_amt else 0 end,0)  AS pub_holi_end_allowance_amt ,    ---- New
----------Holiday allowance for trip start date = weekend
COALESCE(CASE WHEN ((pubhol_start.date is not null OR expense.trip_start_dt =  weekend_start.SUNDAY_DT OR expense.trip_start_dt =  weekend_start.saturday_dt) AND  expense.trip_start_dt = expense.transaction_dt)
									then  holidayref.allowance_amt  ELSE 0 END,0) AS pubhol_startweek_allowance_amt ,         ----------- Step 9
----------Holiday allowance for trip end date = weekend
COALESCE(CASE WHEN ((pubhol_end.date is not null OR expense.trip_end_dt =  weekend_end.SUNDAY_DT OR expense.trip_end_dt =  weekend_end.saturday_dt) AND  expense.trip_end_dt = expense.transaction_dt)
									then  holidayref.allowance_amt  ELSE 0 END,0) AS pubhol_endweek_allowance_amt                ------------ New
FROM
	temp1_fin_tne_travel_allowance_fs  expense		
-------------To Check if trip_start_date is on public holiday
		LEFT JOIN al_finance.fin_tne_ref_public_holidays pubhol_start ON expense.trip_start_dt=pubhol_start.date AND expense.transaction_dt = pubhol_start.date
-------------To Check if trip_end_date is on public holiday
		LEFT JOIN al_finance.fin_tne_ref_public_holidays pubhol_end ON expense.trip_end_dt=pubhol_end.date AND  expense.transaction_dt = pubhol_end.date
-------------To check if trip_start_date is a weekend
		LEFT JOIN (SELECT calendar_dt CALENDAR_DT,week_end_dt SUNDAY_DT,(week_end_dt - interval '1 day') as saturday_dt
								FROM al_finance.fin_date_hier_d ) weekend_start ON expense.trip_start_dt = weekend_start.calendar_dt
-------------To check if trip_start_date is a weekend
		LEFT JOIN (SELECT calendar_dt CALENDAR_DT,week_end_dt SUNDAY_DT,(week_end_dt - interval '1 day') as saturday_dt
								FROM al_finance.fin_date_hier_d ) weekend_end ON expense.trip_end_dt = weekend_end.calendar_dt
--------------To Derive Holiday Allowance
		LEFT JOIN al_finance.fin_tne_ref_public_holiday_allowance holidayref ON expense.TRVL_TYPE=holidayref.trvl_type 
---------------To derive country and city Severity level
		LEFT JOIN al_finance.fin_tne_ref_country_severity_level destination_city ON expense.country_desc=destination_city.country
																						AND expense.city=destination_city.city
---------------To derive country Severity level for other locations
		LEFT JOIN al_finance.fin_tne_ref_country_severity_level destination_all ON expense.country_desc=destination_all.country
																	AND destination_all.city IN ('Other Locations/Construction Sites','All Locations')
---------------To derive global allowance amount based on employee band and number of travel days
		LEFT JOIN al_finance.fin_tne_ref_global_allowance_rate trvlmin_globalallowance ON expense.emp_band=trvlmin_globalallowance.emp_level
					AND ((expense.travel_days_counter BETWEEN trvlmin_globalallowance.trvl_days_min AND trvlmin_globalallowance.trvl_days_max))
---------------To derive global allowance amount based on employee band and number of travel days
		LEFT JOIN al_finance.fin_tne_ref_global_allowance_rate trvlmax_globalallowance ON expense.emp_band=trvlmax_globalallowance.emp_level
						AND (expense.travel_days_counter >=trvlmax_globalallowance.trvl_days_min and trvlmax_globalallowance.trvl_days_max IS NULL)
---------------To derive Forfeit amount based on country and City
		LEFT JOIN al_finance.fin_tne_ref_forfeit forfeit_ref  ON expense.country_cd=forfeit_ref.country_code 
								AND expense.city=forfeit_ref.city
---------------To derive Forfeit amount based on country for other/all locations
		LEFT JOIN al_finance.fin_tne_ref_forfeit forfeit_all  ON expense.country_cd=forfeit_all.country_code  
									AND forfeit_all.city IN ('Other locations','All locations')
---------------To derive Conversion rate to EUR
		LEFT JOIN al_finance.fin_mor_f mor on mor.curncy_cd = CASE when forfeit_ref.amount is not null 
																	then forfeit_ref.currency 
																	else forfeit_all.currency 
																	end 
										and to_char(expense.transaction_dt,'YYYYMM') = mor.period_id
		LEFT JOIN (select period_id,'USD' AS from_curncy_cd,'EUR' as to_curncy_cd,(1/mor_rate) usd_to_eur_rt from al_finance.fin_mor_f where curncy_cd = 'EUR') usdtoeuro
						ON to_char(expense.transaction_dt,'YYYYMM') = usdtoeuro.period_id
		LEFT JOIN al_finance.fin_tne_ref_non_taxable_limit non_tax_limit
											ON expense.trvl_type=non_tax_limit.trvl_type 
												AND expense.travel_allowance_ctgry=non_tax_limit.trvl_allowance_category
) SRC 
) DISTRIBUTED BY (SRC_SYSTEM_ID);
--STEP 5

ANALYZE temp2_fin_tne_travel_allowance_fs;
--STEP 6

INSERT INTO al_finance.fin_tne_travel_allowance_fs
(
sso_id,
employee_id,
glid_company_cd,
trvl_allowance_type,
expense_type,
country_cd,
country_desc,
city,
travel_flag,
transaction_dt,
trip_start_dt, 
trip_end_dt,  
approval_dt,
emp_band,
travel_allowance_ctgry,
trvl_allowance_amt,
trvl_allowance_amt_forfeit,
trvl_allowance_amt_holiday,
trvl_allowance_amt_offshore,
trvl_allowance_amt_total,
trvl_allowance_amt_tax,
trvl_allowance_amt_non_tax,
holiday_taxable_amt,
holiday_non_taxable_amt,
forfeit_taxable_amt,
forfeit_non_taxable_amt,
trvl_type,
holiday_flg,
legal_entity,
payslip_cd,
report_name,
report_dt,
report_key,
report_id,
report_entry_id,
src_system_id,
src_name,
dl_ins_ts,
dl_upd_ts,
dl_ins_by,
dl_upd_by
)
SELECT
expense.sso_id,
expense.employee_id,
expense.glid_company_cd,
expense.trvl_allowance_type,
expense.expense_type,
expense.country_cd,
expense.country_desc,
expense.city,
expense.travel_flag,
expense.transaction_dt,
expense.trip_start_dt, 
expense.trip_end_dt,  
expense.approval_dt,
expense.emp_band,
expense.travel_allowance_ctgry,
round(coalesce(expense.trvl_allowance_amt,0),2) AS trvl_allowance_amt,
round(coalesce(expense.forfeit_trvl_allowance_amt,0),2) AS trvl_allowance_amt_forfeit,
round(coalesce(expense.holiday_trvl_allowance_amt,0),2) AS trvl_allowance_amt_holiday,
round(coalesce(expense.trvl_allowance_amt_offshore,0),2) AS trvl_allowance_amt_offshore,
round(coalesce(expense.trvl_allowance_amt_total,0),2) AS trvl_allowance_amt_total,
round(coalesce(expense.trvl_allowance_amt_tax,0),2) AS trvl_allowance_amt_tax,
round(coalesce(expense.trvl_allowance_amt_non_tax,0),2) AS trvl_allowance_amt_non_tax,
round(coalesce(expense.holiday_taxable_amt,0),2) AS holiday_taxable_amt,
round(coalesce(expense.holiday_non_taxable_amt,0),2) AS holiday_non_taxable_amt,
round(coalesce(expense.forfeit_taxable_amt,0),2) AS forfeit_taxable_amt,
round(coalesce(expense.forfeit_non_taxable_amt,0),2) AS forfeit_non_taxable_amt,
expense.trvl_type,
expense.holiday_flg,
expense.legal_entity,
null AS payslip_cd,
expense.report_name,
expense.report_dt,
expense.report_key,
expense.report_id,
expense.report_entry_id,
expense.src_system_id,
expense.src_name,
expense.dl_ins_ts,
expense.dl_upd_ts,
'fin_tne_mtd_etl_scripts~tne_allw_fact_stg' AS dl_ins_by ,
'fin_tne_mtd_etl_scripts~tne_allw_fact_stg' AS dl_upd_by 
FROM
(
SELECT 
exp_allow.*,
CASE WHEN COALESCE(exp_allow.holiday_trvl_allowance_amt,0) > 0 THEN 'Y' ELSE 'N' END AS holiday_flg,
-------------- Taxable Travel Allowance Amount
CASE WHEN exp_allow.trvl_allowance_type = 'Mission' 
				THEN (COALESCE(exp_allow.trvl_allowance_amt,0) - coalesce(exp_allow.trvl_allowance_amt_non_tax,0))
	WHEN exp_allow.trvl_allowance_type = 'Mission - Offshore' 
				THEN ((COALESCE(exp_allow.trvl_allowance_amt,0) + (coalesce(exp_allow.trvl_allowance_amt,0) * 20.0/100.0)) - coalesce(exp_allow.trvl_allowance_amt_non_tax,0))
	WHEN exp_allow.trvl_allowance_type in ('Mission Mixed Forfeit >30 days' , 'Mission Mixed Forfeit')
				THEN (COALESCE(exp_allow.trvl_allowance_amt,0) - coalesce(exp_allow.trvl_allowance_amt_non_tax,0))
	/*WHEN exp_allow.trvl_allowance_type in ('Relo Assignment & Travel','Relo & Assignment Travel' ,'NPE Mixed Forfeit >30 days')
				THEN (COALESCE(exp_allow.forfeit_trvl_allowance_amt,0)  - coalesce(exp_allow.trvl_allowance_amt_non_tax,0))*/
    ELSE 
		0 
	END 	AS trvl_allowance_amt_tax,
-------------- Taxable Holiday Allowance Amount
CASE WHEN exp_allow.trvl_allowance_type = 'Mission' 
				THEN (COALESCE(exp_allow.holiday_trvl_allowance_amt,0) - coalesce(exp_allow.holiday_non_taxable_amt,0))
	WHEN exp_allow.trvl_allowance_type = 'Mission - Offshore' 
				THEN (COALESCE(exp_allow.holiday_trvl_allowance_amt,0) - coalesce(exp_allow.holiday_non_taxable_amt,0))
	WHEN exp_allow.trvl_allowance_type in ('Mission Mixed Forfeit >30 days' , 'Mission Mixed Forfeit')
				THEN (COALESCE(exp_allow.holiday_trvl_allowance_amt,0) - coalesce(exp_allow.holiday_non_taxable_amt,0))
    ELSE 
		0 
	END 	AS holiday_taxable_amt,
-------------- Taxable Forfeit Allowance Amount
CASE WHEN exp_allow.trvl_allowance_type in ('Mission Mixed Forfeit >30 days' , 'Mission Mixed Forfeit')
				THEN (COALESCE(exp_allow.forfeit_trvl_allowance_amt,0) - coalesce(exp_allow.forfeit_non_taxable_amt,0))
	WHEN exp_allow.trvl_allowance_type in ('Relo Assignment & Travel','Relo & Assignment Travel' ,'NPE Mixed Forfeit >30 days')
				THEN (COALESCE(exp_allow.forfeit_trvl_allowance_amt,0)  - coalesce(exp_allow.forfeit_non_taxable_amt,0))
    ELSE 
		0 
	END 	AS forfeit_taxable_amt
FROM
(
SELECT 
expense_allowance.*,
(coalesce(expense_allowance.trvl_allowance_amt,0) + coalesce(expense_allowance.forfeit_trvl_allowance_amt,0) + coalesce(expense_allowance.holiday_trvl_allowance_amt,0)) AS trvl_allowance_amt_total,
------------ Offshore Travel Allowance Amount
CASE WHEN expense_allowance.trvl_allowance_type = 'Mission - Offshore' 
				THEN COALESCE(expense_allowance.trvl_allowance_amt,0) + (coalesce(expense_allowance.trvl_allowance_amt,0) * 20.0/100.0)
				ELSE 0 
				END AS trvl_allowance_amt_offshore,
-------------- Non Taxable Daily Travel Allowance Amount
CASE WHEN expense_allowance.trvl_allowance_type = 'Mission' 
				THEN 
					CASE 
						WHEN COALESCE(expense_allowance.trvl_allowance_amt,0) <= expense_allowance.non_taxable_limit 
								THEN COALESCE(expense_allowance.trvl_allowance_amt,0)
						ELSE 
							expense_allowance.non_taxable_limit
						END 
	WHEN expense_allowance.trvl_allowance_type = 'Mission - Offshore' 
				THEN 
					CASE 
						WHEN COALESCE(expense_allowance.trvl_allowance_amt,0) + (coalesce(expense_allowance.trvl_allowance_amt,0) * 20.0/100.0) <= expense_allowance.non_taxable_limit 
								THEN COALESCE(expense_allowance.trvl_allowance_amt,0) + (coalesce(expense_allowance.trvl_allowance_amt,0) * 20.0/100.0)
						ELSE 
							expense_allowance.non_taxable_limit
						END 
	WHEN expense_allowance.trvl_allowance_type in ('Mission Mixed Forfeit >30 days' , 'Mission Mixed Forfeit')
				THEN 
					CASE WHEN COALESCE(expense_allowance.forfeit_trvl_allowance_amt,0) <= expense_allowance.non_taxable_limit 
							THEN 
								CASE 
								WHEN COALESCE(expense_allowance.trvl_allowance_amt,0) <= (expense_allowance.non_taxable_limit - COALESCE(expense_allowance.forfeit_trvl_allowance_amt,0)) 
								  THEN 
									COALESCE(expense_allowance.trvl_allowance_amt,0)
								 ELSE  
									expense_allowance.non_taxable_limit - COALESCE(expense_allowance.forfeit_trvl_allowance_amt,0)
									END
							ELSE 
								0
							END 
	/*WHEN expense_allowance.trvl_allowance_type in ('Relo Assignment & Travel','Relo & Assignment Travel' ,'NPE Mixed Forfeit >30 days')
				THEN 
					CASE 
						WHEN COALESCE(expense_allowance.forfeit_trvl_allowance_amt,0) <= expense_allowance.non_taxable_limit 
								THEN 
									COALESCE(expense_allowance.forfeit_trvl_allowance_amt,0)
					ELSE 
						expense_allowance.non_taxable_limit
					END 	*/
    ELSE 
		0 
	END 	AS trvl_allowance_amt_non_tax,
--------------  Forfeit Non-Taxable Amount
CASE WHEN expense_allowance.trvl_allowance_type in ('Mission Mixed Forfeit >30 days' , 'Mission Mixed Forfeit')
				THEN 
					CASE WHEN COALESCE(expense_allowance.forfeit_trvl_allowance_amt,0) <= expense_allowance.non_taxable_limit 
							THEN 
								COALESCE(expense_allowance.forfeit_trvl_allowance_amt,0)
							ELSE 
								expense_allowance.non_taxable_limit
							END 
	WHEN expense_allowance.trvl_allowance_type in ('Relo Assignment & Travel','Relo & Assignment Travel' ,'NPE Mixed Forfeit >30 days')
				THEN 
					CASE 
						WHEN COALESCE(expense_allowance.forfeit_trvl_allowance_amt,0) <= expense_allowance.non_taxable_limit 
								THEN 
									COALESCE(expense_allowance.forfeit_trvl_allowance_amt,0)
					ELSE 
						expense_allowance.non_taxable_limit
					END 	
    ELSE 
		0 
	END 	AS forfeit_non_taxable_amt,
--------------  Holiday non taxable amount
CASE WHEN expense_allowance.trvl_allowance_type = 'Mission' AND COALESCE(expense_allowance.holiday_trvl_allowance_amt,0) > 0
						THEN CASE WHEN COALESCE(expense_allowance.trvl_allowance_amt,0) <= expense_allowance.non_taxable_limit 
									THEN 
									CASE WHEN COALESCE(holiday_trvl_allowance_amt,0) >= expense_allowance.non_taxable_limit - (COALESCE(expense_allowance.trvl_allowance_amt,0))
										THEN 
										expense_allowance.non_taxable_limit - (COALESCE(expense_allowance.trvl_allowance_amt,0))
										ELSE COALESCE(expense_allowance.holiday_trvl_allowance_amt,0)
									END
								ELSE 0
							END					   
	WHEN expense_allowance.trvl_allowance_type = 'Mission - Offshore' AND COALESCE(expense_allowance.holiday_trvl_allowance_amt,0) > 0
				THEN 
						 CASE 
								WHEN COALESCE(expense_allowance.trvl_allowance_amt,0) + (coalesce(expense_allowance.trvl_allowance_amt,0) * 20.0/100.0) <= expense_allowance.non_taxable_limit 
									THEN 
									CASE WHEN COALESCE(holiday_trvl_allowance_amt,0) >= expense_allowance.non_taxable_limit - (COALESCE(expense_allowance.trvl_allowance_amt,0) + (coalesce(expense_allowance.trvl_allowance_amt,0) * 20.0/100.0))
										THEN 
										expense_allowance.non_taxable_limit - (COALESCE(expense_allowance.trvl_allowance_amt,0) + (coalesce(expense_allowance.trvl_allowance_amt,0) * 20.0/100.0))
										ELSE COALESCE(expense_allowance.holiday_trvl_allowance_amt,0)
									END
								ELSE 0
							END		
	WHEN expense_allowance.trvl_allowance_type in ('Mission Mixed Forfeit >30 days' , 'Mission Mixed Forfeit')  AND COALESCE(expense_allowance.holiday_trvl_allowance_amt,0) > 0
				THEN 
						CASE 
								WHEN (COALESCE(expense_allowance.forfeit_trvl_allowance_amt,0) + COALESCE(expense_allowance.trvl_allowance_amt,0)) <= expense_allowance.non_taxable_limit 
									THEN 
										CASE WHEN COALESCE(holiday_trvl_allowance_amt,0) >= expense_allowance.non_taxable_limit - (COALESCE(expense_allowance.forfeit_trvl_allowance_amt,0) + COALESCE(expense_allowance.trvl_allowance_amt,0))
										THEN 
										expense_allowance.non_taxable_limit - (COALESCE(expense_allowance.forfeit_trvl_allowance_amt,0) + COALESCE(expense_allowance.trvl_allowance_amt,0))
										ELSE COALESCE(expense_allowance.holiday_trvl_allowance_amt,0)
									END
								ELSE 0
							END	
    ELSE 
		0 
	END 	AS holiday_non_taxable_amt
FROM 
temp2_fin_tne_travel_allowance_fs expense_allowance
) exp_allow
) expense;
--STEP 7

ANALYZE al_finance.fin_tne_travel_allowance_fs;
