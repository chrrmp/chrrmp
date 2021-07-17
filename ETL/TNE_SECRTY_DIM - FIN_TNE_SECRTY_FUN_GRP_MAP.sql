--STEP 1

UPDATE AL_FINANCE.FIN_TNE_SECRTY_FUN_GRP_MAP final
SET 
emp_id  		=t.sso,
function 		=t.function,
active_flag     =t.active_flag,
custom_field_1  =t.custom_field_1,
custom_field_2  =t.custom_field_2,
custom_field_3  =t.custom_field_3,
custom_field_4  =t.custom_field_4,
custom_field_5  =t.custom_field_5,
dl_upd_ts       =CURRENT_TIMESTAMP
from 
(SELECT cntl.sso as sso,cntl.function as function, cntl.active_flag as active_flag, cntl.custom_field_1 as custom_field_1, 
                                                            cntl.custom_field_2 as custom_field_2, cntl.custom_field_3 as custom_field_3, cntl.custom_field_4 as custom_field_4, 
                                                            cntl.custom_field_5 as custom_field_5                                  
                                                                           FROM 
                                                                            (SELECT usr.*, row_number() over (partition by sso,function order by dl_upd_ts desc) as rn 
                                                                                          from  GOG_DATAFORMS.FIN_TNE_SECURITY_USER_REGISTRATION  usr 
                                                                                                         INNER JOIN (SELECT max(coalesce(last_update_date,'1900-01-01')) as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl 
                                                                                                                        WHERE group_name = 'TNE_SECRTY_DIM' AND table_name ='FIN_TNE_SECRTY_FUN_GRP_MAP') lud on 1=1
                                                                           WHERE 
                                                                           usr.dl_upd_ts >= lud.last_update_date and usr.security_type='Function' AND  usr.workflow_status='Approved'
                                                                                          and usr.function !='ALL'
                                                            ) cntl WHERE cntl.rn=1
                              ) t  
 where final.src_system_id = t.sso||'~'||t.function;
--STEP 2

UPDATE AL_FINANCE.FIN_TNE_SECRTY_FUN_GRP_MAP final
SET 
emp_id  		=t.sso,
function 		=t.job_family_grp,
active_flag     =t.active_flag,
dl_upd_ts       =CURRENT_TIMESTAMP
from   (SELECT src.sso as sso,src.job_family_grp as job_family_grp,src.active_flag as active_flag
                                             FROM 
                                             (
                                             (SELECT  distinct usr.sso as sso,usr.function as function,usr.active_flag as active_flag
                                             from  GOG_DATAFORMS.FIN_TNE_SECURITY_USER_REGISTRATION  usr
                                             INNER JOIN (SELECT max(coalesce(last_update_date,'1900-01-01')) as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl 
                                                               WHERE group_name = 'TNE_SECRTY_DIM' AND table_name ='FIN_TNE_SECRTY_FUN_GRP_MAP') lud on 1=1  
                                                               where usr.function='ALL' AND USR.dl_upd_ts >= lud.last_update_date 
                                                               and usr.security_type='Function' AND  usr.workflow_status='Approved') cntl   
                                                CROSS JOIN  (select Distinct job_family_grp from al_finance.fin_employee_d WHERE job_family_grp IS NOT NULL  ) BS       
                                              ) src
                              ) T      
where final.src_system_id =t.sso||'~'||t.job_family_grp ;
--STEP 3

INSERT INTO  AL_FINANCE.FIN_TNE_SECRTY_FUN_GRP_MAP
(emp_id  
,function
,active_flag 
,custom_field_1
,custom_field_2
,custom_field_3
,custom_field_4
,custom_field_5
,src_system_id
,src_name
,dl_ins_by  
,dl_ins_ts  
,dl_upd_by
,dl_upd_ts )
SELECT 
 cntl.sso
,cntl.function
,cntl.active_flag
,cntl.custom_field_1  
,cntl.custom_field_2  
,cntl.custom_field_3  
,cntl.custom_field_4  
,cntl.custom_field_5 
,cntl.sso||'~'||cntl.function as src_system_id
,'Dataform' as src_name 
, cntl.dl_ins_by  
,CURRENT_TIMESTAMP AS dl_ins_ts  
,cntl.dl_upd_by
,CURRENT_TIMESTAMP AS dl_upd_ts
  FROM 
 (SELECT usr.*, row_number() over (partition by sso,function order by dl_upd_ts desc) as rn 
 from  GOG_DATAFORMS.FIN_TNE_SECURITY_USER_REGISTRATION  usr
 INNER JOIN (SELECT max(coalesce(last_update_date,'1900-01-01')) as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl 
        WHERE group_name = 'TNE_SECRTY_DIM' AND table_name ='FIN_TNE_SECRTY_FUN_GRP_MAP') lud on 1=1
		where  usr.dl_upd_ts >= lud.last_update_date and usr.security_type='Function' AND  usr.workflow_status='Approved' and usr.function !='ALL') cntl      
 LEFT JOIN  AL_FINANCE.FIN_TNE_SECRTY_FUN_GRP_MAP abc on abc.src_system_id= cntl.sso||'~'||cntl.function 
WHERE rn=1 AND abc.src_system_id IS NULL 
 
 UNION 
 
 SELECT 
 cntl.sso
,BS.job_family_grp
,cntl.active_flag
,NULL AS custom_field_1  
,NULL AS custom_field_2  
,NULL AS custom_field_3  
,NULL AS custom_field_4  
,NULL AS custom_field_5 
,cntl.sso||'~'||BS.job_family_grp as src_system_id
,'Dataform' as src_name 
, 'Dataform' as dl_ins_by  
,CURRENT_TIMESTAMP AS dl_ins_ts  
,'Dataform' as dl_upd_by
,CURRENT_TIMESTAMP AS dl_upd_ts
  FROM 
 (SELECT  distinct usr.sso as sso, usr.function as function,usr.active_flag as active_flag
 from  GOG_DATAFORMS.FIN_TNE_SECURITY_USER_REGISTRATION  usr
 INNER JOIN (SELECT max(coalesce(last_update_date,'1900-01-01')) as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl 
       WHERE group_name = 'TNE_SECRTY_DIM' AND table_name ='FIN_TNE_SECRTY_FUN_GRP_MAP') lud on 1=1  
       where usr.FUNCTION='ALL' AND  usr.dl_upd_ts >= lud.last_update_date and usr.security_type='Function' AND  usr.workflow_status='Approved') cntl   
   CROSS JOIN  (select Distinct job_family_grp from al_finance.fin_employee_d WHERE job_family_grp IS NOT NULL   ) BS       
 LEFT JOIN  AL_FINANCE.FIN_TNE_SECRTY_FUN_GRP_MAP abc on abc.src_system_id= cntl.sso||'~'||BS.job_family_grp  
WHERE  abc.src_system_id IS NULL ;
