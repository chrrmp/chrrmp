--STEP 1

SET OPTIMIZER=ON;
--STEP 2

TRUNCATE TABLE al_finance.fin_employee_ds;
--STEP 3

INSERT INTO al_finance.fin_employee_ds
(
emp_id,
business_grp_name,
sub_business,
business_segment,
display_name,
email_address,
emp_home_regn,
emp_first_name,
emp_full_name,
emp_func,
job_family,
hr_supervisor_id,
hr_supervisor_name,
job_name,
emp_last_name,
latitude,
longitude,
operatng_ledger_buc,
operatng_ledger_compny_cd,
operatng_ledger_cc_cd,
operatng_ledger_pl_cd,
operatng_ledger_proj_cd,
operatng_ledger_ref_cd,
par_func,
job_family_grp,
position_name,
reporting_level,
sso_id,
workday_user_name,
status,
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
person_start_dt,
person_end_dt,
eff_assignmnt_start_dt,
eff_assignmnt_end_dt,
cc_start_dt,
cc_end_dt,
disposition_flag,
working_department,
office_address,
address_line_1,
address_line_2,
address_line_3,
city,
state_province,
cntry_cd,
postal_cd,
assignment_attrib_10,
band_grp,
src_system_id,
src_name,
dl_ins_by,
dl_upd_by,
dl_ins_ts,
dl_upd_ts,
local_band
)
select
		LPAD(emp_dtls.emp_id,8,'0') as Employee_Id,
		emp_wrk.sub_busn_desc as Business_grp_name,
		emp_wrk.sub_busn_desc as Sub_Business,
		emp_wrk.busn_seg_desc as Business_Segment,
		emp_dtls.knwn_as as Display_Name,
		emp_dtls.prim_work_email_addr as Email_Address,
		dim_loc.loc_region as Employee_Home_Region,
		emp_dtls.first_nm as First_Name,
		emp_dtls.emp_nm as Full_Name,
		dim_job.job_family as emp_func,
		dim_job.job_family as Job_Family,
		emp_dtls.hr_partner_emp_id as HR_Supervisor_ID,
		emp_dtls.hr_partner_nm as HR_Supervisor_Name,
		dim_job.job_title as Job_Title,
		emp_dtls.last_nm as Last_Name,
		null as Latitude,
		null as Longitude,
		ch.ol_buc as Operating_Ledger_BUC,
		ch.ol_co_cd as Operating_Ledger_Company_Code,
		case
		when emp_dtls.co_hrchy = 'Legacy O&G' then
		case
			when ch.emp_id is not null then ch.legacy_cost_ctr::text
			else null
			end
		else emp_wrk.cost_ctr_ref_id::text
			end as Operating_Ledger_Cost_Center,
		ch.ol_pl as Operating_Ledger_Product_Line,
		ch.ol_project_cd as Operating_Ledger_Project_Code,
		ch.ol_ref_cd as Operating_Ledger_Ref_Code,
		dim_job.job_family_group as par_func,
		dim_job.job_family_group as Job_Family_Group,
		emp_wrk.position_nm as Position_Name,
		null as Reporting_Level,
		emp_dtls.ge_sso_id as GE_SSO,
		emp_dtls.workday_user_nm as Workday_User_Name,
		emp_wrk.emp_stat as Status,
		null as Supervisor_Name_Level_00,
		emp_wrk.mgr_nm as Supervisor_Name_Level_01,
		dim_oh.mgr_lvl2_first_nm || ' ' || mgr_lvl2_last_nm as Supervisor_Name_Level_02,
		dim_oh.mgr_lvl3_first_nm || ' ' || mgr_lvl3_last_nm as Supervisor_Name_Level_03,
		dim_oh.mgr_lvl4_first_nm || ' ' || mgr_lvl4_last_nm as Supervisor_Name_Level_04,
		dim_oh.mgr_lvl5_first_nm || ' ' || mgr_lvl5_last_nm as Supervisor_Name_Level_05,
		dim_oh.mgr_lvl6_first_nm || ' ' || mgr_lvl6_last_nm as Supervisor_Name_Level_06,
		dim_oh.mgr_lvl7_first_nm || ' ' || mgr_lvl7_last_nm as Supervisor_Name_Level_07,
		dim_oh.mgr_lvl8_first_nm || ' ' || mgr_lvl8_last_nm as Supervisor_Name_Level_08,
		dim_oh.mgr_lvl9_first_nm || ' ' || mgr_lvl9_last_nm as Supervisor_Name_Level_09,
		dim_oh.mgr_lvl10_first_nm || ' ' || mgr_lvl10_last_nm as Supervisor_Name_Level_10,
		dim_oh.mgr_lvl11_first_nm || ' ' || mgr_lvl11_last_nm as Supervisor_Name_Level_11,
		dim_oh.mgr_lvl12_first_nm || ' ' || mgr_lvl12_last_nm as Supervisor_Name_Level_12,
		dim_oh.mgr_lvl13_first_nm || ' ' || mgr_lvl13_last_nm as Supervisor_Name_Level_13,
		dim_oh.mgr_lvl14_first_nm || ' ' || mgr_lvl14_last_nm as Supervisor_Name_Level_14,
		dim_oh.mgr_lvl15_first_nm || ' ' || mgr_lvl15_last_nm as Supervisor_Name_Level_15,
		null as Supervisor_Name_Level_16,
		null as Supervisor_Name_Level_17,
		null as Supervisor_Name_Level_18,
		null as Supervisor_Name_Level_19,
		null as Supervisor_Id_Level_02,
		emp_dtls1.workday_user_nm as Supervisor_Id_Level_01,
		emp_dtls2.workday_user_nm as Supervisor_Id_Level_02,
		emp_dtls3.workday_user_nm as Supervisor_Id_Level_03,
		emp_dtls4.workday_user_nm as Supervisor_Id_Level_04,
		emp_dtls5.workday_user_nm as Supervisor_Id_Level_05,
		emp_dtls6.workday_user_nm as Supervisor_Id_Level_06,
		emp_dtls7.workday_user_nm as Supervisor_Id_Level_07,
		emp_dtls8.workday_user_nm as Supervisor_Id_Level_08,
		emp_dtls9.workday_user_nm as Supervisor_Id_Level_09,
		emp_dtls10.workday_user_nm as Supervisor_Id_Level_10,
		emp_dtls11.workday_user_nm as Supervisor_Id_Level_11,
		emp_dtls12.workday_user_nm as Supervisor_Id_Level_12,
		emp_dtls13.workday_user_nm as Supervisor_Id_Level_13,
		emp_dtls14.workday_user_nm as Supervisor_Id_Level_14,
		emp_dtls15.workday_user_nm as Supervisor_Id_Level_15,
		null as Supervisor_Id_Level_16,
		null as Supervisor_Id_Level_17,
		null as Supervisor_Id_Level_18,
		null as Supervisor_Id_Level_19,
		null as Territory_Short_Name,
		'Employee' as User_Person_Type,
		null as Work_Telephone,
		emp_dtls.hire_dt as Person_Start_Date,
		emp_wrk.termination_eff_dt as Person_End_Date,
		null as Effective_Assignment_Start_Date,
		null as Effective_Assignment_End_Date,
		ch.costing_eff_dt as Cost_Center_Start_Date,
		null as Cost_Center_End_Date,
		null as Disposition_Flag,
		null as Working_Department,
		dim_loc.loc_desc as Office_Address,
		dim_loc.loc_addr_line1 as Address_Line_1,
		dim_loc.loc_addr_line2 as Address_Line_2,
		dim_loc.loc_addr_line3 as Address_Line_3,
		dim_loc.loc_city as City,
		dim_loc.loc_st_region as State_Province,
		dim_loc.loc_cntry as Country,
		dim_loc.loc_postal_cd as Postal_Code,
		emp_wrk.org_desc as Assignment_Attribute10,
		null as Band_Group,
		LPAD(emp_dtls.emp_id,8,'0') as src_system_id,
		'HRDL' as src_name,
		'fin_tne_mtd_etl_scripts~emp_dim_stg',
		'fin_tne_mtd_etl_scripts~emp_dim_stg',
		current_timestamp,
		current_timestamp,
		emp_wrk.co_lvl as local_band
	from
	og_hrdl.dim_emp_dtls emp_dtls	
	inner join og_hrdl.emp_work_hist emp_wrk on emp_wrk.emp_skey = emp_dtls.emp_skey and end_dt= '9999-12-31' and( ( emp_stat <> 'Terminated' )
																					or ( emp_stat = 'Terminated' and termination_eff_dt >= current_date - 180 ) )
	left join og_hrdl.emp_costing_hist ch on emp_dtls.emp_id::text = ch.emp_ID::text	and ch.end_dt = '9999-12-31'
	left join og_hrdl.dim_job dim_job on		emp_wrk.job_cd = dim_job.job_cd
	left join og_hrdl.dim_location dim_loc on		emp_wrk.loc_ref_id = dim_loc.loc_ref_id
	left join og_hrdl.dim_sup_org_hrchy dim_oh on		emp_dtls.emp_skey = dim_oh.emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls1 on		emp_dtls1.emp_id = dim_oh.mgr_id
	left join og_hrdl.dim_emp_dtls emp_dtls2 on		emp_dtls2.emp_skey = dim_oh.mgr_lvl2_emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls3 on		emp_dtls3.emp_skey = dim_oh.mgr_lvl3_emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls4 on		emp_dtls4.emp_skey = dim_oh.mgr_lvl4_emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls5 on		emp_dtls5.emp_skey = dim_oh.mgr_lvl5_emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls6 on		emp_dtls6.emp_skey = dim_oh.mgr_lvl6_emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls7 on		emp_dtls7.emp_skey = dim_oh.mgr_lvl7_emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls8 on		emp_dtls8.emp_skey = dim_oh.mgr_lvl8_emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls9 on		emp_dtls9.emp_skey = dim_oh.mgr_lvl9_emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls10 on		emp_dtls10.emp_skey = dim_oh.mgr_lvl10_emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls11 on		emp_dtls11.emp_skey = dim_oh.mgr_lvl11_emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls12 on		emp_dtls12.emp_skey = dim_oh.mgr_lvl12_emp_skey	
	left join og_hrdl.dim_emp_dtls emp_dtls13 on		emp_dtls13.emp_skey = dim_oh.mgr_lvl13_emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls14 on		emp_dtls14.emp_skey = dim_oh.mgr_lvl14_emp_skey
	left join og_hrdl.dim_emp_dtls emp_dtls15 on		emp_dtls15.emp_skey = dim_oh.mgr_lvl15_emp_skey
	/*WHERE
		(
	emp_dtls.etl_update_dt >= (SELECT coalesce(last_update_date,'1900-01-01') as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl
									WHERE group_name = 'EMP_DIM_STG' AND table_name ='FIN_EMPLOYEE_DS') 
	OR
	emp_wrk.etl_update_dt >= (SELECT coalesce(last_update_date,'1900-01-01') as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl
									WHERE group_name = 'EMP_DIM_STG' AND table_name ='FIN_EMPLOYEE_DS') 
				)*/
	;
--STEP 4

INSERT INTO al_finance.fin_employee_ds
(
emp_id,
business_grp_name,
sub_business,
business_segment,
display_name,
email_address,
emp_home_regn,
emp_first_name,
emp_full_name,
emp_func,
job_family,
hr_supervisor_id,
hr_supervisor_name,
job_name,
emp_last_name,
latitude,
longitude,
operatng_ledger_buc,
operatng_ledger_compny_cd,
operatng_ledger_cc_cd,
operatng_ledger_pl_cd,
operatng_ledger_proj_cd,
operatng_ledger_ref_cd,
par_func,
job_family_grp,
position_name,
reporting_level,
sso_id,
workday_user_name,
status,
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
person_start_dt,
person_end_dt,
eff_assignmnt_start_dt,
eff_assignmnt_end_dt,
cc_start_dt,
cc_end_dt,
disposition_flag,
working_department,
office_address,
address_line_1,
address_line_2,
address_line_3,
city,
state_province,
cntry_cd,
postal_cd,
assignment_attrib_10,
band_grp,
src_system_id,
src_name,
dl_ins_by,
dl_upd_by,
dl_ins_ts,
dl_upd_ts,
local_band
)
with cw_hist as (
	select
		*
	from
		(
		select 	* ,	row_number() over (partition by emp_id	order by end_dt desc) as rnum
		from
			og_hrdl.cw_emp_work_hist cw_emp_wrk
		where
			((cw_emp_wrk.emp_stat = 'Active' and cw_emp_wrk.Termination_eff_dt is null) or (cw_emp_wrk.Termination_eff_dt >= current_date-180)) )x
		where
		rnum = 1 ) 
 select
	cw_emp_dtls.emp_id as Employee_Id,
	emp_wrk.sub_busn_cd as business_grp_name,
	emp_wrk.sub_busn_cd as Sub_Business,
	emp_wrk.busn_seg_desc as Business_Segment,
	null as Display_Name,
	cw_emp_dtls.prim_work_email_addr as Email_Address,
	dim_loc.loc_region as Employee_Home_Region,
	cw_emp_dtls.first_nm as First_Name,
	cw_emp_dtls.emp_nm as Full_Name,
	dim_job.job_family as emp_func,
	dim_job.job_family as Job_Family,
	null as HR_Supervisor_ID,
	null as HR_Supervisor_Name,
	dim_job.job_title as Job_Title,
	cw_emp_dtls.last_nm as Last_Name,
	null as Latitude,
	null as Longitude,
    ch.ol_buc as Operating_Ledger_BUC,
    ch.ol_co_cd as Operating_Ledger_Company_Code,
	case when emp_dtls.co_hrchy = 'Legacy O&G' then 
		case when ch.emp_id is not null then ch.legacy_cost_ctr::text 
	else null end else emp_wrk.cost_ctr_ref_id::text 
	end as Operating_Ledger_Cost_Center,
	ch.ol_pl as Operating_Ledger_Product_Line,
	ch.ol_project_cd as Operating_Ledger_Project_Code,
	ch.ol_ref_cd as Operating_Ledger_Ref_Code,
	dim_job.job_family_group as par_func,
	dim_job.job_family_group as Job_Family_Group,
	null as Position_Name,
	null as Reporting_Level,
	cw_emp_dtls.ge_sso_id as GE_SSO,
    null as Workday_User_Name,
	cw_emp_wrk.emp_stat::text as Status,
	null as Supervisor_Name_Level_00,
	emp_dtls.emp_nm as Supervisor_Name_Level_01,
	dim_oh.mgr_lvl2_first_nm || ' ' || mgr_lvl2_last_nm as Supervisor_Name_Level_02,
	dim_oh.mgr_lvl3_first_nm || ' ' || mgr_lvl3_last_nm as Supervisor_Name_Level_03,
	dim_oh.mgr_lvl4_first_nm || ' ' || mgr_lvl4_last_nm as Supervisor_Name_Level_04,
	dim_oh.mgr_lvl5_first_nm || ' ' || mgr_lvl5_last_nm as Supervisor_Name_Level_05,
	dim_oh.mgr_lvl6_first_nm || ' ' || mgr_lvl6_last_nm as Supervisor_Name_Level_06,
	dim_oh.mgr_lvl7_first_nm || ' ' || mgr_lvl7_last_nm as Supervisor_Name_Level_07,
	dim_oh.mgr_lvl8_first_nm || ' ' || mgr_lvl8_last_nm as Supervisor_Name_Level_08,
	dim_oh.mgr_lvl9_first_nm || ' ' || mgr_lvl9_last_nm as Supervisor_Name_Level_09,
	dim_oh.mgr_lvl10_first_nm || ' ' || mgr_lvl10_last_nm as Supervisor_Name_Level_10,
	dim_oh.mgr_lvl11_first_nm || ' ' || mgr_lvl11_last_nm as Supervisor_Name_Level_11,
	dim_oh.mgr_lvl12_first_nm || ' ' || mgr_lvl12_last_nm as Supervisor_Name_Level_12,
	dim_oh.mgr_lvl13_first_nm || ' ' || mgr_lvl13_last_nm as Supervisor_Name_Level_13,
	dim_oh.mgr_lvl14_first_nm || ' ' || mgr_lvl14_last_nm as Supervisor_Name_Level_14,
	dim_oh.mgr_lvl15_first_nm || ' ' || mgr_lvl15_last_nm as Supervisor_Name_Level_15,
	null as Supervisor_Name_Level_16,
	null as Supervisor_Name_Level_17,
	null as Supervisor_Name_Level_18,
	null as Supervisor_Name_Level_19,
	null as Supervisor_Id_Level_00,
	emp_dtls1.workday_user_nm as  Supervisor_Id_Level_01,
	emp_dtls2.workday_user_nm as  Supervisor_Id_Level_02,
	emp_dtls3.workday_user_nm as  Supervisor_Id_Level_03,
	emp_dtls4.workday_user_nm as  Supervisor_Id_Level_04,
	emp_dtls5.workday_user_nm as  Supervisor_Id_Level_05,
	emp_dtls6.workday_user_nm as  Supervisor_Id_Level_06,
	emp_dtls7.workday_user_nm as  Supervisor_Id_Level_07,
	emp_dtls8.workday_user_nm as  Supervisor_Id_Level_08,
	emp_dtls9.workday_user_nm as  Supervisor_Id_Level_09,
	emp_dtls10.workday_user_nm as  Supervisor_Id_Level_10,
	emp_dtls11.workday_user_nm as  Supervisor_Id_Level_11,
	emp_dtls12.workday_user_nm as  Supervisor_Id_Level_12,
	emp_dtls13.workday_user_nm as  Supervisor_Id_Level_13,
	emp_dtls14.workday_user_nm as  Supervisor_Id_Level_14,
	emp_dtls15.workday_user_nm as  Supervisor_Id_Level_15,
	null as Supervisor_Id_Level_16,
	null as Supervisor_Id_Level_17,
	null as Supervisor_Id_Level_18,
	null as Supervisor_Id_Level_19,
	null as Territory_Short_Name,
	'Contractor' as User_Person_Type,
	null as Work_Telephone,
	null as Person_Start_Date,
	null as Person_End_Date,
	cw_emp_wrk.asgnmnt_eff_start_dt as Effective_Assignment_Start_Date,
	cw_emp_wrk.asgnmnt_eff_end_dt as Effective_Assignment_End_Date,
	ch.costing_eff_dt as Cost_Center_Start_Date,
	null as Cost_Center_End_Date,
	null as Disposition_Flag,
	null as Working_Department,
	dim_loc.loc_desc as Office_Address,
	dim_loc.loc_addr_line1 as Address_Line_1,
	dim_loc.loc_addr_line2 as Address_Line_2,
	dim_loc.loc_addr_line3 as Address_Line_3,
	dim_loc.loc_city as City,
	dim_loc.loc_st_region as State_Province,
	dim_loc.loc_cntry as Country,
	dim_loc.loc_postal_cd as Postal_Code,
	emp_wrk.org_desc as Assignment_Attribute10,
	null as Band_Group,
	cw_emp_dtls.emp_id as src_system_id,
	'HRDL' as src_name,
	'fin_tne_mtd_etl_scripts~emp_dim_stg',
	'fin_tne_mtd_etl_scripts~emp_dim_stg',
	current_timestamp,
	current_timestamp,
	emp_wrk.co_lvl as local_band
from
	og_hrdl.dim_cw_emp_dtls cw_emp_dtls
left join cw_hist  cw_emp_wrk on	cw_emp_dtls.emp_skey = cw_emp_wrk.emp_skey
left join og_hrdl.dim_job dim_job on	cw_emp_wrk.job_skey = dim_job.job_skey
left join og_hrdl.dim_location dim_loc on	cw_emp_wrk.loc_skey = dim_loc.loc_skey
left join og_hrdl.dim_emp_dtls emp_dtls on	cw_emp_wrk.mgr_id::text = emp_dtls.emp_id::text
join og_hrdl.emp_work_hist emp_wrk on	emp_dtls.emp_skey = emp_wrk.emp_skey	and emp_wrk.end_dt= '9999-12-31'
left join og_hrdl.dim_sup_org_hrchy dim_oh on	emp_dtls.emp_skey = dim_oh.emp_skey
left join og_hrdl.emp_costing_hist ch on	emp_dtls.emp_id::text = ch.emp_ID::text	and ch.end_dt = '9999-12-31'
left join og_hrdl.dim_emp_dtls emp_dtls1 on	emp_dtls1.emp_id = dim_oh.mgr_id
left join og_hrdl.dim_emp_dtls emp_dtls2 on	emp_dtls2.emp_skey = dim_oh.mgr_lvl2_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls3 on	emp_dtls3.emp_skey = dim_oh.mgr_lvl3_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls4 on	emp_dtls4.emp_skey = dim_oh.mgr_lvl4_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls5 on	emp_dtls5.emp_skey = dim_oh.mgr_lvl5_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls6 on	emp_dtls6.emp_skey = dim_oh.mgr_lvl6_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls7 on	emp_dtls7.emp_skey = dim_oh.mgr_lvl7_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls8 on	emp_dtls8.emp_skey = dim_oh.mgr_lvl8_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls9 on	emp_dtls9.emp_skey = dim_oh.mgr_lvl9_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls10 on	emp_dtls10.emp_skey = dim_oh.mgr_lvl10_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls11 on	emp_dtls11.emp_skey = dim_oh.mgr_lvl11_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls12 on	emp_dtls12.emp_skey = dim_oh.mgr_lvl12_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls13 on	emp_dtls13.emp_skey = dim_oh.mgr_lvl13_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls14 on	emp_dtls14.emp_skey = dim_oh.mgr_lvl14_emp_skey
left join og_hrdl.dim_emp_dtls emp_dtls15 on	emp_dtls15.emp_skey = dim_oh.mgr_lvl15_emp_skey
	/*WHERE
		(
	cw_emp_dtls.etl_update_dt >= (SELECT coalesce(last_update_date,'1900-01-01') as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl
									WHERE group_name = 'EMP_DIM_STG' AND table_name ='FIN_EMPLOYEE_DS') 
	OR
	emp_wrk.etl_update_dt >= (SELECT coalesce(last_update_date,'1900-01-01') as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl
									WHERE group_name = 'EMP_DIM_STG' AND table_name ='FIN_EMPLOYEE_DS') 
				)*/
;
--STEP 5

ANALYZE al_finance.fin_employee_ds;
