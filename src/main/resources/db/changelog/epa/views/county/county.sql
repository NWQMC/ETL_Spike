create or replace view ${WQX_SCHEMA_NAME}.county as
select distinct
       "CNTY_UID" cnty_uid,
       "ST_UID" st_uid,
       "CNTY_NAME" cnty_name,
       "CNTY_FIPS_CD" cnty_fips_cd,
       "CNTY_LAST_CHANGE_DATE" cnty_last_change_date,
       "USR_UID_LAST_CHANGE" usr_uid_last_change
  from ${WQX_SCHEMA_NAME}."COUNTY"