--STEP 1

UPDATE AL_FINANCE.FIN_TNE_SECRTY_REGN_GRP_MAP final
SET 
emp_id  		=t.sso,
region 			=t.region,
active_flag     =t.active_flag,
custom_field_1  =t.custom_field_1,
custom_field_2  =t.custom_field_2,
custom_field_3  =t.custom_field_3,
custom_field_4  =t.custom_field_4,
custom_field_5  =t.custom_field_5,
dl_upd_ts       =CURRENT_TIMESTAMP
from 
(SELECT cntl.sso as sso,cntl.region as region, cntl.active_flag as active_flag, cntl.custom_field_1 as custom_field_1, 
                                                            cntl.custom_field_2 as custom_field_2, cntl.custom_field_3 as custom_field_3, cntl.custom_field_4 as custom_field_4, 
                                                            cntl.custom_field_5 as custom_field_5                                  
                                                                           FROM 
                                                                            (SELECT usr.*, row_number() over (partition by sso,region order by dl_upd_ts desc) as rn 
                                                                                          from  GOG_DATAFORMS.FIN_TNE_SECURITY_USER_REGISTRATION  usr 
                                                                                                         INNER JOIN (SELECT max(coalesce(last_update_date,'1900-01-01')) as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl 
                                                                                                                        WHERE group_name = 'TNE_SECRTY_DIM' AND table_name ='FIN_TNE_SECRTY_REGN_GRP_MAP') lud on 1=1
                                                                           WHERE 
                                                                           usr.dl_upd_ts >= lud.last_update_date and usr.security_type='O&G Region' AND  usr.workflow_status='Approved'
                                                                                          and usr.region !='ALL'
                                                            ) cntl WHERE cntl.rn=1
                              ) t  
 where final.src_system_id = t.sso||'~'||t.region;
--STEP 2

 UPDATE AL_FINANCE.FIN_TNE_SECRTY_REGN_GRP_MAP final
SET 
emp_id  		=t.sso,
region 			=t.EMP_HOME_REGN,
active_flag     =t.active_flag,
dl_upd_ts       =CURRENT_TIMESTAMP
from   (SELECT src.sso as sso,src.EMP_HOME_REGN as EMP_HOME_REGN,src.active_flag as active_flag
                                             FROM 
                                             (
                                             (SELECT  distinct usr.sso as sso,usr.Region as region,usr.active_flag as active_flag
                                             from  GOG_DATAFORMS.FIN_TNE_SECURITY_USER_REGISTRATION  usr
                                             INNER JOIN (SELECT max(coalesce(last_update_date,'1900-01-01')) as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl 
                                                               WHERE group_name = 'TNE_SECRTY_DIM' AND table_name ='FIN_TNE_SECRTY_REGN_GRP_MAP') lud on 1=1  
                                                               where usr.region='ALL' AND USR.dl_upd_ts >= lud.last_update_date 
                                                               and usr.region='O&G Region' AND  usr.workflow_status='Approved') cntl   
												CROSS JOIN  (SELECT DISTINCT (case
												when upper(EMP.CNTRY_CD) in ('BANGLADESH','BHUTAN','INDIA','SRI LANKA','MALDIVES','NEPAL') then 'India'
												when upper(EMP.CNTRY_CD) in ('CHINA','HONG KONG','MACAO SAR','MONGOLIA','PROVINCE OF CHINA-TAIWAN','TAIWAN') then 'China'
												else EMP.EMP_HOME_REGN end) as EMP_HOME_REGN FROM AL_FINANCE.FIN_EMPLOYEE_D EMP WHERE EMP_HOME_REGN IS NOT NULL   ) BS           
                                              ) src
                              ) T      
where final.src_system_id =t.sso||'~'||t.EMP_HOME_REGN ;
--STEP 3

INSERT INTO  AL_FINANCE.FIN_TNE_SECRTY_REGN_GRP_MAP
(emp_id  
,region
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
,dl_upd_ts
 )
SELECT DISTINCT
 cntl.sso
,cntl.region
,cntl.active_flag
,cntl.custom_field_1  
,cntl.custom_field_2  
,cntl.custom_field_3  
,cntl.custom_field_4  
,cntl.custom_field_5 
,cntl.sso||'~'||cntl.region as src_system_id
,'Dataform' as src_name 
,cntl.dl_ins_by  
,CURRENT_TIMESTAMP AS dl_ins_ts  
,cntl.dl_upd_by
,CURRENT_TIMESTAMP AS dl_upd_ts
  FROM 
 (SELECT usr.*, row_number() over (partition by sso,region order by dl_upd_ts desc) as rn 
 from  GOG_DATAFORMS.FIN_TNE_SECURITY_USER_REGISTRATION  usr
 INNER JOIN (SELECT max(coalesce(last_update_date,'1900-01-01')) as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl 
        WHERE group_name = 'TNE_SECRTY_DIM' AND table_name ='FIN_TNE_SECRTY_REGN_GRP_MAP') lud on 1=1
		where  usr.dl_upd_ts >= lud.last_update_date and usr.security_type='O&G Region' AND  usr.workflow_status='Approved' and usr.region !='ALL') cntl      
 LEFT JOIN  AL_FINANCE.FIN_TNE_SECRTY_REGN_GRP_MAP abc on abc.src_system_id= cntl.sso||'~'||cntl.region 
WHERE rn=1 AND src_system_id IS NULL 
 
 UNION 
 
 SELECT 
 cntl.sso
,BS.EMP_HOME_REGN
,cntl.active_flag
,null as custom_field_1  
,null as custom_field_2  
,null as custom_field_3  
,null as custom_field_4  
,null as custom_field_5 
,cntl.sso||'~'||BS.EMP_HOME_REGN as src_system_id
,'Dataform' as src_name 
,'Dataform' as dl_ins_by  
,CURRENT_TIMESTAMP AS dl_ins_ts  
,'Dataform' as dl_upd_by
,CURRENT_TIMESTAMP AS dl_upd_ts
  FROM 
  (SELECT  distinct usr.sso as sso, usr.region as region,usr.active_flag as active_flag
 from  GOG_DATAFORMS.FIN_TNE_SECURITY_USER_REGISTRATION  usr
 INNER JOIN (SELECT max(coalesce(last_update_date,'1900-01-01')) as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl 
       WHERE group_name = 'TNE_SECRTY_DIM' AND table_name ='FIN_TNE_SECRTY_REGN_GRP_MAP') lud on 1=1  
       where usr.region='ALL' AND  USR.dl_upd_ts >= lud.last_update_date and usr.security_type='O&G Region' AND  usr.workflow_status='Approved') cntl   
   CROSS JOIN  (SELECT DISTINCT (case
 when upper(EMP.CNTRY_CD) in ('BANGLADESH','BHUTAN','INDIA','SRI LANKA','MALDIVES','NEPAL') then 'India'
 when upper(EMP.CNTRY_CD) in ('CHINA','HONG KONG','MACAO SAR','MONGOLIA','PROVINCE OF CHINA-TAIWAN','TAIWAN') then 'China'
 else EMP.EMP_HOME_REGN end) as EMP_HOME_REGN FROM AL_FINANCE.FIN_EMPLOYEE_D EMP WHERE EMP_HOME_REGN IS NOT NULL   ) BS       
 LEFT JOIN  AL_FINANCE.FIN_TNE_SECRTY_REGN_GRP_MAP abc on abc.src_system_id= cntl.sso||'~'||BS.EMP_HOME_REGN  
WHERE  abc.src_system_id IS NULL ;
