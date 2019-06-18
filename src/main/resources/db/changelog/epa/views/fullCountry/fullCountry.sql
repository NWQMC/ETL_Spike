create or replace view ${WQX_SCHEMA_NAME}.full_country (country_code, country_name) as
select distinct
       coalesce(nwis.country_cd, wqx."CNTRY_CD") country_code,
       coalesce(nwis.country_nm, wqx."CNTRY_NAME") country_name
  from ${NWIS_SCHEMA_NAME}.country nwis
       full outer join ${WQX_SCHEMA_NAME}."COUNTRY" wqx
         on nwis.country_cd = wqx."CNTRY_CD"
