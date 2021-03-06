--STEP 1

UPDATE al_finance.fin_employee_d dim
SET 
emp_id                                 =     ds.emp_id,
status                                 =     ds.status,
band_grp                                 =     ds.band_grp,
business_grp_name                                 =     ds.business_grp_name,
business_segment                                 =     ds.business_segment,
display_name                                 =     ds.display_name,
email_address                                 =     ds.email_address,
emp_home_regn                                 =     ds.emp_home_regn,
expense_grp                                 =     ds.expense_grp,
emp_first_name                                 =     ds.emp_first_name,
emp_full_name                                 =     ds.emp_full_name,
emp_last_name                                 =     ds.emp_last_name,
emp_func                                 =     ds.emp_func,
job_family                                 =     ds.job_family,
hr_supervisor_id                                 =     ds.hr_supervisor_id,
hr_supervisor_name                                 =     ds.hr_supervisor_name,
ifg_cd                                 =     ds.ifg_cd,
job_name                                 =     ds.job_name,
latitude                                 =     ds.latitude,
longitude                                 =     ds.longitude,
operatng_ledger_buc                                 =     ds.operatng_ledger_buc,
operatng_ledger_compny_cd                                 =     ds.operatng_ledger_compny_cd,
operatng_ledger_cc_cd                                 =     ds.operatng_ledger_cc_cd,
operatng_ledger_pl_cd                                 =     ds.operatng_ledger_pl_cd,
operatng_ledger_proj_cd                                 =     ds.operatng_ledger_proj_cd,
operatng_ledger_ref_cd                                 =     ds.operatng_ledger_ref_cd,
sso_id                                 =     ds.sso_id,
workday_user_name                                 =     ds.workday_user_name,
par_func                                 =     ds.par_func,
job_family_grp                                 =     ds.job_family_grp,
position_name                                 =     ds.position_name,
postal_cd                                 =     ds.postal_cd,
reporting_level                                 =     ds.reporting_level,
supervisor_id                                 =     ds.supervisor_id,
supervisor_name                                 =     ds.supervisor_name,
supervisor_name_lvl_00                                 =     ds.supervisor_name_lvl_00,
supervisor_name_lvl_01                                 =     ds.supervisor_name_lvl_01,
supervisor_name_lvl_02                                 =     ds.supervisor_name_lvl_02,
supervisor_name_lvl_03                                 =     ds.supervisor_name_lvl_03,
supervisor_name_lvl_04                                 =     ds.supervisor_name_lvl_04,
supervisor_name_lvl_05                                 =     ds.supervisor_name_lvl_05,
supervisor_name_lvl_06                                 =     ds.supervisor_name_lvl_06,
supervisor_name_lvl_07                                 =     ds.supervisor_name_lvl_07,
supervisor_name_lvl_08                                 =     ds.supervisor_name_lvl_08,
supervisor_name_lvl_09                                 =     ds.supervisor_name_lvl_09,
supervisor_name_lvl_10                                 =     ds.supervisor_name_lvl_10,
supervisor_name_lvl_11                                 =     ds.supervisor_name_lvl_11,
supervisor_name_lvl_12                                 =     ds.supervisor_name_lvl_12,
supervisor_name_lvl_13                                 =     ds.supervisor_name_lvl_13,
supervisor_name_lvl_14                                 =     ds.supervisor_name_lvl_14,
supervisor_name_lvl_15                                 =     ds.supervisor_name_lvl_15,
supervisor_name_lvl_16                                 =     ds.supervisor_name_lvl_16,
supervisor_name_lvl_17                                 =     ds.supervisor_name_lvl_17,
supervisor_name_lvl_18                                 =     ds.supervisor_name_lvl_18,
supervisor_name_lvl_19                                 =     ds.supervisor_name_lvl_19,
supervisor_id_lvl_00                                 =     ds.supervisor_id_lvl_00,
supervisor_id_lvl_01                                 =     ds.supervisor_id_lvl_01,
supervisor_id_lvl_02                                 =     ds.supervisor_id_lvl_02,
supervisor_id_lvl_03                                 =     ds.supervisor_id_lvl_03,
supervisor_id_lvl_04                                 =     ds.supervisor_id_lvl_04,
supervisor_id_lvl_05                                 =     ds.supervisor_id_lvl_05,
supervisor_id_lvl_06                                 =     ds.supervisor_id_lvl_06,
supervisor_id_lvl_07                                 =     ds.supervisor_id_lvl_07,
supervisor_id_lvl_08                                 =     ds.supervisor_id_lvl_08,
supervisor_id_lvl_09                                 =     ds.supervisor_id_lvl_09,
supervisor_id_lvl_10                                 =     ds.supervisor_id_lvl_10,
supervisor_id_lvl_11                                 =     ds.supervisor_id_lvl_11,
supervisor_id_lvl_12                                 =     ds.supervisor_id_lvl_12,
supervisor_id_lvl_13                                 =     ds.supervisor_id_lvl_13,
supervisor_id_lvl_14                                 =     ds.supervisor_id_lvl_14,
supervisor_id_lvl_15                                 =     ds.supervisor_id_lvl_15,
supervisor_id_lvl_16                                 =     ds.supervisor_id_lvl_16,
supervisor_id_lvl_17                                 =     ds.supervisor_id_lvl_17,
supervisor_id_lvl_18                                 =     ds.supervisor_id_lvl_18,
supervisor_id_lvl_19                                 =     ds.supervisor_id_lvl_19,
territory_short_name                                 =     ds.territory_short_name,
user_person_type                                 =     ds.user_person_type,
work_telephone                                 =     ds.work_telephone,
supervisor_sso_lvl_all                                 =     ds.supervisor_sso_lvl_all,
person_start_dt                                 =     ds.person_start_dt,
person_end_dt                                 =     ds.person_end_dt,
eff_assignmnt_start_dt                                 =     ds.eff_assignmnt_start_dt,
eff_assignmnt_end_dt                                 =     ds.eff_assignmnt_end_dt,
cc_start_dt                                 =     ds.cc_start_dt,
cc_end_dt                                 =     ds.cc_end_dt,
rptg_ifg                                 =     ds.rptg_ifg,
disposition_flag                                 =     ds.disposition_flag,
person_name                                 =     ds.person_name,
industry                                 =     ds.industry,
segment                                 =     ds.segment,
sub_business                                 =     ds.sub_business,
working_department                                 =     ds.working_department,
office_address                                 =     ds.office_address,
address_line                                 =     ds.address_line,
address_line_1                                 =     ds.address_line_1,
address_line_2                                 =     ds.address_line_2,
address_line_3                                 =     ds.address_line_3,
cntry_cd                                 =     ds.cntry_cd,
city                                 =     ds.city,
state_province                                 =     ds.state_province,
assignment_attrib_10                                 =     ds.assignment_attrib_10,
band_grp_mapping                                 =     ds.band_grp_mapping,
local_band                                 =     ds.local_band,
dl_upd_ts                                 =     current_timestamp
FROM al_finance.fin_employee_ds ds
WHERE
dim.src_system_id = ds.src_system_id;
--STEP 2

INSERT INTO al_finance.fin_employee_d
(
emp_id,
status,
band_grp,
business_grp_name,
business_segment,
display_name,
email_address,
emp_home_regn,
expense_grp,
emp_first_name,
emp_full_name,
emp_last_name,
emp_func,
job_family,
hr_supervisor_id,
hr_supervisor_name,
ifg_cd,
job_name,
latitude,
longitude,
operatng_ledger_buc,
operatng_ledger_compny_cd,
operatng_ledger_cc_cd,
operatng_ledger_pl_cd,
operatng_ledger_proj_cd,
operatng_ledger_ref_cd,
sso_id,
workday_user_name,
par_func,
job_family_grp,
position_name,
postal_cd,
reporting_level,
supervisor_id,
supervisor_name,
supervisor_name_lvl_00,
supervisor_name_lvl_01,
supervisor_name_lvl_02,
supervisor_name_lvl_03,
supervisor_name_lvl_04,
supervisor_name_lvl_05,
supervisor_name_lvl_06,
supervisor_name_lvl_07,
supervisor_name_lvl_08,
supervisor_name_lvl_09,
supervisor_name_lvl_10,
supervisor_name_lvl_11,
supervisor_name_lvl_12,
supervisor_name_lvl_13,
supervisor_name_lvl_14,
supervisor_name_lvl_15,
supervisor_name_lvl_16,
supervisor_name_lvl_17,
supervisor_name_lvl_18,
supervisor_name_lvl_19,
supervisor_id_lvl_00,
supervisor_id_lvl_01,
supervisor_id_lvl_02,
supervisor_id_lvl_03,
supervisor_id_lvl_04,
supervisor_id_lvl_05,
supervisor_id_lvl_06,
supervisor_id_lvl_07,
supervisor_id_lvl_08,
supervisor_id_lvl_09,
supervisor_id_lvl_10,
supervisor_id_lvl_11,
supervisor_id_lvl_12,
supervisor_id_lvl_13,
supervisor_id_lvl_14,
supervisor_id_lvl_15,
supervisor_id_lvl_16,
supervisor_id_lvl_17,
supervisor_id_lvl_18,
supervisor_id_lvl_19,
territory_short_name,
user_person_type,
work_telephone,
supervisor_sso_lvl_all,
person_start_dt,
person_end_dt,
eff_assignmnt_start_dt,
eff_assignmnt_end_dt,
cc_start_dt,
cc_end_dt,
rptg_ifg,
disposition_flag,
person_name,
industry,
segment,
sub_business,
working_department,
office_address,
address_line,
address_line_1,
address_line_2,
address_line_3,
cntry_cd,
city,
state_province,
assignment_attrib_10,
band_grp_mapping,
src_system_id,
src_name,
dl_ins_by,
dl_upd_by,
dl_ins_ts,
dl_upd_ts,
local_band
)
SELECT 
ds.emp_id,
ds.status,
ds.band_grp,
ds.business_grp_name,
ds.business_segment,
ds.display_name,
ds.email_address,
ds.emp_home_regn,
ds.expense_grp,
ds.emp_first_name,
ds.emp_full_name,
ds.emp_last_name,
ds.emp_func,
ds.job_family,
ds.hr_supervisor_id,
ds.hr_supervisor_name,
ds.ifg_cd,
ds.job_name,
ds.latitude,
ds.longitude,
ds.operatng_ledger_buc,
ds.operatng_ledger_compny_cd,
ds.operatng_ledger_cc_cd,
ds.operatng_ledger_pl_cd,
ds.operatng_ledger_proj_cd,
ds.operatng_ledger_ref_cd,
ds.sso_id,
ds.workday_user_name,
ds.par_func,
ds.job_family_grp,
ds.position_name,
ds.postal_cd,
ds.reporting_level,
ds.supervisor_id,
ds.supervisor_name,
ds.supervisor_name_lvl_00,
ds.supervisor_name_lvl_01,
ds.supervisor_name_lvl_02,
ds.supervisor_name_lvl_03,
ds.supervisor_name_lvl_04,
ds.supervisor_name_lvl_05,
ds.supervisor_name_lvl_06,
ds.supervisor_name_lvl_07,
ds.supervisor_name_lvl_08,
ds.supervisor_name_lvl_09,
ds.supervisor_name_lvl_10,
ds.supervisor_name_lvl_11,
ds.supervisor_name_lvl_12,
ds.supervisor_name_lvl_13,
ds.supervisor_name_lvl_14,
ds.supervisor_name_lvl_15,
ds.supervisor_name_lvl_16,
ds.supervisor_name_lvl_17,
ds.supervisor_name_lvl_18,
ds.supervisor_name_lvl_19,
ds.supervisor_id_lvl_00,
ds.supervisor_id_lvl_01,
ds.supervisor_id_lvl_02,
ds.supervisor_id_lvl_03,
ds.supervisor_id_lvl_04,
ds.supervisor_id_lvl_05,
ds.supervisor_id_lvl_06,
ds.supervisor_id_lvl_07,
ds.supervisor_id_lvl_08,
ds.supervisor_id_lvl_09,
ds.supervisor_id_lvl_10,
ds.supervisor_id_lvl_11,
ds.supervisor_id_lvl_12,
ds.supervisor_id_lvl_13,
ds.supervisor_id_lvl_14,
ds.supervisor_id_lvl_15,
ds.supervisor_id_lvl_16,
ds.supervisor_id_lvl_17,
ds.supervisor_id_lvl_18,
ds.supervisor_id_lvl_19,
ds.territory_short_name,
ds.user_person_type,
ds.work_telephone,
ds.supervisor_sso_lvl_all,
ds.person_start_dt,
ds.person_end_dt,
ds.eff_assignmnt_start_dt,
ds.eff_assignmnt_end_dt,
ds.cc_start_dt,
ds.cc_end_dt,
ds.rptg_ifg,
ds.disposition_flag,
ds.person_name,
ds.industry,
ds.segment,
ds.sub_business,
ds.working_department,
ds.office_address,
ds.address_line,
ds.address_line_1,
ds.address_line_2,
ds.address_line_3,
ds.cntry_cd,
ds.city,
ds.state_province,
ds.assignment_attrib_10,
ds.band_grp_mapping,
ds.src_system_id,
ds.src_name,
'fin_tne_mtd_etl_scripts~emp_dim',
'fin_tne_mtd_etl_scripts~emp_dim',
current_timestamp,
current_timestamp,
ds.local_band
FROM
al_finance.fin_employee_ds ds
LEFT JOIN al_finance.fin_employee_d dim ON ds.src_system_id = dim.src_system_id
WHERE
dim.src_system_id is null;
--STEP 3

VACUUM ANALYZE al_finance.fin_employee_d;
