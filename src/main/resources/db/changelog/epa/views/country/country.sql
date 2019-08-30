create or replace view ${WQX_SCHEMA_NAME}.country as
select distinct
       "CNTRY_UID" cntry_uid,
       "CNTRY_CD" cntry_cd,
       "CNTRY_NAME" cntry_name,
       "CNTRY_LAST_CHANGE_DATE" cntry_last_change_date,
       "USR_UID_LAST_CHANGE" usr_uid_last_change
  from ${WQX_DUMP_SCHEMA_NAME}."COUNTRY"
