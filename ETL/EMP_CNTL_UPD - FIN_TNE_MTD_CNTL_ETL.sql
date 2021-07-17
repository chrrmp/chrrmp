--STEP 1

UPDATE al_finance.fin_tne_mtd_cntl_etl
set last_update_date = (select min(dl_upd_ts) from al_finance.fin_employee_ds)
WHERE
group_name = 'EMP_DIM_STG' and table_name = 'FIN_EMPLOYEE_DS';
