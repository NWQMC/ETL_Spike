create or replace view ${WQX_SCHEMA_NAME}.state as
select distinct
       "ST_UID" st_uid,
       "CNTRY_UID" cntry_uid,
       "ST_CD" st_cd,
       "ST_NAME" st_name,
       "ST_FIPS_CD" st_fips_cd,
       "ST_LAST_CHANGE_DATE" st_last_change_date,
       "ER_UID" er_uid,
       "USR_UID_LAST_CHANGE" usr_uid_last_change
  from ${WQX_DUMP_SCHEMA_NAME}."STATE"