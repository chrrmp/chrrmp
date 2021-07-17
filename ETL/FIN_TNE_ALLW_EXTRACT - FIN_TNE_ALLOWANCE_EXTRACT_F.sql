--STEP 1

CREATE TEMP TABLE fin_tne_travel_allowance_f_temp
AS (SELECT f.*,(f.payroll_year::varchar||lpad(f.payroll_month::varchar,2,'0'))::varchar as period_id from al_finance.fin_tne_travel_allowance_f f WHERE f.extract_status is null)
DISTRIBUTED BY (src_system_id);
--STEP 2

ANALYZE fin_tne_travel_allowance_f_temp;
--STEP 3

CREATE TEMP TABLE fin_tne_allowance_derive_file_nbr
AS (
select t.period_id,t.legal_entity_id,COALESCE((SELECT max(file_id) from al_finance.fin_tne_allowance_extract_f),0)+row_number() over(order by t.period_id::int) as file_id,
coalesce(fsn.file_seq_nbr,0)+row_number() over(partition by t.period_id,t.legal_entity_id order by t.period_id,t.legal_entity_id) as file_seq_nbr
FROM (
SELECT distinct period_id,legal_entity_id
from fin_tne_travel_allowance_f_temp 
WHERE extract_status is null 
) T 
left join (SELECT period_id,legal_entity_id,max(file_seq_nbr) as file_seq_nbr FROM al_finance.fin_tne_allowance_extract_f GROUP by period_id,legal_entity_id) fsn 
						ON t.period_id = fsn.period_id AND t.legal_entity_id = fsn.legal_entity_id
) DISTRIBUTED BY (period_id,legal_entity_id);
--STEP 4

ANALYZE fin_tne_allowance_derive_file_nbr;
--STEP 5

INSERT INTO al_finance.fin_tne_allowance_extract_f
(
data_extract,
record_id,
file_id,
legal_entity_id,
period_id,
file_seq_nbr,
file_genratn_status,
src_system_id,
src_name,
dl_upd_ts,
dl_ins_ts,
dl_upd_by,
dl_ins_by
)
SELECT 
data_extract,
record_id,
file_id,
legal_entity_id,
period_id,
file_seq_nbr,
'Pending' as file_genratn_status,
file_id||'~'||legal_entity_id||'~'||period_id||'~'||file_seq_nbr as src_system_id,
'CON' as src_name,
current_timestamp,
current_timestamp,
'fin_tne_mtd_etl_scripts~fin_tne_allw_extract',
'fin_tne_mtd_etl_scripts~fin_tne_allw_extract'
FROM
(
SELECT DISTINCT
'R1'||lpad(der_nbr.file_id::varchar,10,'0')||temp.legal_entity_id||temp.period_id||lpad(der_nbr.file_seq_nbr::varchar,2,'0') as data_extract,
'R1' as record_id,
der_nbr.file_id as file_id,
temp.legal_entity_id as legal_entity_id,
temp.period_id as period_id,
der_nbr.file_seq_nbr as file_seq_nbr
FROM 
fin_tne_travel_allowance_f_temp temp
join fin_tne_allowance_derive_file_nbr der_nbr
ON der_nbr.period_id = temp.period_id and der_nbr.legal_entity_id = temp.legal_entity_id
) T;
--STEP 6

INSERT INTO al_finance.fin_tne_allowance_extract_f
(
data_extract,
record_id,
file_id,
legal_entity_id,
payslip_item_record_id,
sso_id,
emp_id,
payslip_cd,
period_id,
total_allowance_amt,
amt_flag,
file_genratn_status,
file_seq_nbr,
src_system_id,
src_name,
dl_upd_ts,
dl_ins_ts,
dl_upd_by,
dl_ins_by
)

SELECT 
data_extract,
record_id,
file_id,
legal_entity_id,
payslip_item_record_id,
emp_sso_id,
employee_id,
payslip_code,
period_id,
travel_allowance_payslip_amt,
amt_flag,
'Pending' as file_genratn_status,
file_seq_nbr,
file_id||'~'||legal_entity_id||'~'||period_id||'~'||file_seq_nbr||'~'||payslip_code||'~'||payslip_item_record_id as src_system_id,
'CON' as src_name,
current_timestamp,
current_timestamp,
'fin_tne_mtd_etl_scripts~fin_tne_allw_extract',
'fin_tne_mtd_etl_scripts~fin_tne_allw_extract'
FROM
(
SELECT DISTINCT
'R2'||lpad(der_nbr.file_id::varchar,10,'0')||temp.legal_entity_id
||LPAD((row_number() over(partition by temp.period_id,temp.legal_entity_id order by emp_sso_id,payslip_code))::varchar,4,'0')
||emp_sso_id||' '||payslip_code
||temp.period_id
||LPAD(replace(travel_allowance_payslip_amt::varchar,'.',''),10,'0')
||case when travel_allowance_payslip_amt < 0 then 0 else 1 end
||'000000' as data_extract,
'R2' as record_id,
der_nbr.file_id as file_id,
temp.legal_entity_id as legal_entity_id,
row_number() over(partition by temp.period_id,temp.legal_entity_id order by emp_sso_id,payslip_code) as payslip_item_record_id,
emp_sso_id,
employee_id,
payslip_code,
temp.period_id as period_id,
travel_allowance_payslip_amt,
case when travel_allowance_payslip_amt < 0 then 0 else 1 end as amt_flag,
der_nbr.file_seq_nbr as file_seq_nbr
FROM 
fin_tne_travel_allowance_f_temp temp
join fin_tne_allowance_derive_file_nbr der_nbr
ON der_nbr.period_id = temp.period_id and der_nbr.legal_entity_id = temp.legal_entity_id
) T;
--STEP 7

INSERT INTO al_finance.fin_tne_allowance_extract_f
(
data_extract,
record_id,
file_id,
legal_entity_id,
payslip_cd,
period_id,
payslip_item_record_cnt,
total_allowance_amt,
amt_flag,
file_seq_nbr,
file_genratn_status,
src_system_id,
src_name,
dl_upd_ts,
dl_ins_ts,
dl_upd_by,
dl_ins_by
)
SELECT 
data_extract,
record_id,
file_id,
legal_entity_id,
payslip_code,
period_id,
payslip_item_record_cnt,
travel_allowance_payslip_amt,
amt_flag,
file_seq_nbr,
'Pending' as file_genratn_status,
file_id||'~'||legal_entity_id||'~'||'~'||payslip_code as src_system_id,
'CON' as src_name,
current_timestamp,
current_timestamp,
'fin_tne_mtd_etl_scripts~fin_tne_allw_extract',
'fin_tne_mtd_etl_scripts~fin_tne_allw_extract'
FROM
(
SELECT 
'R3'||lpad(der_nbr.file_id::varchar,10,'0')||temp.legal_entity_id
||temp.payslip_code
||LPAD(count(*)::varchar,4,'0')
||LPAD(replace(sum(temp.travel_allowance_payslip_amt)::varchar,'.',''),12,'0')
||case when sum(temp.travel_allowance_payslip_amt) < 0 then 0 else 1 end as data_extract,
'R3' as record_id,
der_nbr.file_id as file_id,
temp.legal_entity_id as legal_entity_id,
temp.payslip_code,
temp.period_id as period_id,
count(*) as payslip_item_record_cnt,
sum(temp.travel_allowance_payslip_amt) as travel_allowance_payslip_amt,
case when sum(travel_allowance_payslip_amt) < 0 then 0 else 1 end as amt_flag,
der_nbr.file_seq_nbr as file_seq_nbr
FROM 
fin_tne_travel_allowance_f_temp temp
join fin_tne_allowance_derive_file_nbr der_nbr
ON der_nbr.period_id = temp.period_id and der_nbr.legal_entity_id = temp.legal_entity_id
GROUP BY der_nbr.file_id,temp.legal_entity_id,temp.payslip_code,temp.period_id,der_nbr.file_seq_nbr
) T;
--STEP 8

INSERT INTO al_finance.fin_tne_allowance_extract_f
(
data_extract,
record_id,
file_id,
legal_entity_id,
period_id,
file_seq_nbr,
file_genratn_status,
src_system_id,
src_name,
dl_upd_ts,
dl_ins_ts,
dl_upd_by,
dl_ins_by
)
SELECT 
data_extract,
record_id,
file_id,
legal_entity_id,
period_id,
file_seq_nbr,
'Pending' as file_genratn_status,
file_id||'~'||legal_entity_id||'~'||period_id||'~'||file_seq_nbr as src_system_id,
'CON' as src_name,
current_timestamp,
current_timestamp,
'fin_tne_mtd_etl_scripts~fin_tne_allw_extract',
'fin_tne_mtd_etl_scripts~fin_tne_allw_extract'
FROM
(
SELECT DISTINCT
'R9' as data_extract,
'R9' as record_id,
der_nbr.file_id as file_id,
temp.legal_entity_id as legal_entity_id,
temp.period_id as period_id,
der_nbr.file_seq_nbr as file_seq_nbr
FROM 
fin_tne_travel_allowance_f_temp temp
join fin_tne_allowance_derive_file_nbr der_nbr
ON der_nbr.period_id = temp.period_id and der_nbr.legal_entity_id = temp.legal_entity_id
) T;
--STEP 9

UPDATE al_finance.fin_tne_travel_allowance_f src_fact
SET extract_status = 'Completed'
FROM fin_tne_travel_allowance_f_temp temp
WHERE 
src_fact.src_system_id  = temp.src_system_id;
