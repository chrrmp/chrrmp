--STEP 1

UPDATE AL_FINANCE.FIN_TNE_SECRTY_BUSINESS_SEGMNT_MAP final
SET 
emp_id =t.sso,
business_segment =t.product_company,
active_flag     =t.active_flag,
custom_field_1  =t.custom_field_1,
custom_field_2  =t.custom_field_2,
custom_field_3  =t.custom_field_3,
custom_field_4  =t.custom_field_4,
custom_field_5  =t.custom_field_5,
dl_upd_ts       =CURRENT_TIMESTAMP
from (SELECT cntl.sso as sso,cntl.product_company as product_company, cntl.active_flag as active_flag, cntl.custom_field_1 as custom_field_1, 
                                                            cntl.custom_field_2 as custom_field_2, cntl.custom_field_3 as custom_field_3, cntl.custom_field_4 as custom_field_4, 
                                                            cntl.custom_field_5 as custom_field_5                                  
                                                                           FROM 
                                                                            (SELECT usr.*, row_number() over (partition by sso,product_company order by dl_upd_ts desc) as rn 
                                                                                          from  GOG_DATAFORMS.FIN_TNE_SECURITY_USER_REGISTRATION  usr 
                                                                                                         INNER JOIN (SELECT max(coalesce(last_update_date,'1900-01-01')) as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl 
                                                                                                                        WHERE group_name = 'TNE_SECRTY_DIM' AND table_name ='FIN_TNE_SECRTY_BUSINESS_SEGMNT_MAP') lud on 1=1
                                                                           WHERE 
                                                                           usr.dl_upd_ts >= lud.last_update_date and usr.security_type='Product Company' AND  usr.workflow_status='Approved'
                                                                                          and usr.product_company !='ALL'
                                                            ) cntl WHERE cntl.rn=1
                              ) t  
 where final.src_system_id = t.sso||'~'||t.product_company;
--STEP 2

UPDATE AL_FINANCE.FIN_TNE_SECRTY_BUSINESS_SEGMNT_MAP final
SET 
emp_id  =t.sso,
business_segment =t.business_segment,
active_flag     =t.active_flag,
dl_upd_ts       =CURRENT_TIMESTAMP
from   (SELECT src.sso as sso,src.business_segment as business_segment,src.active_flag as active_flag
                                             FROM 
                                             (
                                             (SELECT  distinct usr.sso as sso,usr.product_company as product_company,usr.active_flag as active_flag
                                             from  GOG_DATAFORMS.FIN_TNE_SECURITY_USER_REGISTRATION  usr
                                             INNER JOIN (SELECT max(coalesce(last_update_date,'1900-01-01')) as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl 
                                                               WHERE group_name = 'TNE_SECRTY_DIM' AND table_name ='FIN_TNE_SECRTY_BUSINESS_SEGMNT_MAP') lud on 1=1  
                                                               where usr.product_company='ALL' AND USR.dl_upd_ts >= lud.last_update_date 
                                                               and usr.security_type='Product Company' AND  usr.workflow_status='Approved') cntl   
                                                CROSS JOIN  (select Distinct business_segment from al_finance.fin_employee_d WHERE business_segment IS NOT NULL  ) BS       
                                              ) src
                              ) T      
where final.src_system_id =t.sso||'~'||t.business_segment ;
--STEP 3

INSERT INTO  AL_FINANCE.FIN_TNE_SECRTY_BUSINESS_SEGMNT_MAP
(emp_id  
,business_segment  
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
,cntl.product_company
,cntl.active_flag
,cntl.custom_field_1  
,cntl.custom_field_2  
,cntl.custom_field_3  
,cntl.custom_field_4  
,cntl.custom_field_5 
,cntl.sso||'~'||cntl.product_company as src_system_id
,'Dataform' as src_name 
,cntl.dl_ins_by  
,CURRENT_TIMESTAMP AS dl_ins_ts  
,cntl.dl_upd_by
,CURRENT_TIMESTAMP AS dl_upd_ts
FROM 
                (SELECT usr.*, row_number() over (partition by sso,product_company order by dl_upd_ts desc) as rn 
                from  GOG_DATAFORMS.FIN_TNE_SECURITY_USER_REGISTRATION  usr 
                INNER JOIN (SELECT max(coalesce(last_update_date,'1900-01-01')) as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl 
                                             WHERE group_name = 'TNE_SECRTY_DIM' AND table_name ='FIN_TNE_SECRTY_BUSINESS_SEGMNT_MAP') lud on 1=1
                              WHERE 
                              usr.dl_upd_ts >= lud.last_update_date and usr.security_type='Product Company' AND  usr.workflow_status='Approved'
                              and usr.product_company !='ALL'
               ) cntl      
 LEFT JOIN  AL_FINANCE.FIN_TNE_SECRTY_BUSINESS_SEGMNT_MAP abc on abc.src_system_id= cntl.sso||'~'||cntl.Product_company 
WHERE cntl.rn=1 AND abc.src_system_id IS NULL 

UNION

select 
CNTL.SSO,
BS.business_segment
,cntl.active_flag
,null as custom_field_1  
,null as custom_field_2  
,null as custom_field_3  
,null as custom_field_4  
,null as custom_field_5 
,cntl.sso||'~'||BS.business_segment as src_system_id
,'Dataform' as src_name 
,'Dataform' as dl_ins_by  
,CURRENT_TIMESTAMP AS dl_ins_ts  
,'Dataform' as dl_upd_by
,CURRENT_TIMESTAMP AS dl_upd_ts
FROM 
 (SELECT  distinct usr.sso as sso,usr.product_company as product_company,usr.active_flag as active_flag
from  GOG_DATAFORMS.FIN_TNE_SECURITY_USER_REGISTRATION  usr
INNER JOIN (SELECT max(coalesce(last_update_date,'1900-01-01')) as last_update_date FROM al_finance.fin_tne_mtd_cntl_etl 
       WHERE group_name = 'TNE_SECRTY_DIM' AND table_name ='FIN_TNE_SECRTY_BUSINESS_SEGMNT_MAP') lud on 1=1  
       where usr.product_company='ALL' AND 
         USR.dl_upd_ts >= lud.last_update_date and usr.security_type='Product Company' AND  usr.workflow_status='Approved') cntl   
   CROSS JOIN  (select Distinct business_segment from al_finance.fin_employee_d WHERE business_segment IS NOT NULL  ) BS       
 LEFT JOIN  AL_FINANCE.FIN_TNE_SECRTY_BUSINESS_SEGMNT_MAP abc on abc.src_system_id= cntl.sso||'~'||BS.business_segment 
WHERE  abc.src_system_id IS NULL ;
