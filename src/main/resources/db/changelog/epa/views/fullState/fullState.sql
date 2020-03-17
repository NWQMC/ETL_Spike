create or replace view ${WQX_SCHEMA_NAME}.full_state (state_code, state_name) as
select distinct
       coalesce(nwis.country_cd, wqx.cntry_cd) || ':' ||
       coalesce(nwis.state_cd, wqx.st_fips_cd) state_code,
       coalesce(nwis.state_nm, wqx.st_name) state_name
  from ${NWIS_SCHEMA_NAME}.state nwis
       full outer join (
                        select country."CNTRY_CD" cntry_cd,
                               to_char(state."ST_FIPS_CD", 'fm00') st_fips_cd,
                               state."ST_NAME" st_name
                          from ${WQX_DUMP_SCHEMA_NAME}."COUNTRY" country
                               join ${WQX_DUMP_SCHEMA_NAME}."STATE" state
                                 on country."CNTRY_UID" = state."CNTRY_UID"
                       ) wqx
         on nwis.country_cd = wqx.cntry_cd and
            nwis.state_cd = wqx.st_fips_cd