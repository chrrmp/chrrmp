--STEP 1

UPDATE AL_FINANCE.FIN_TNE_MTD_CNTL_ETL
set last_update_date = (select max(dl_upd_ts) - interval '3 hours' from gog_dataforms.FIN_TNE_SECURITY_USER_REGISTRATION)
WHERE
group_name = 'TNE_SECRTY_DIM';
