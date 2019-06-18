create or replace view ${WQX_SCHEMA_NAME}.full_county (county_code, county_name) as
select distinct
       coalesce(nwis.country_cd, wqx.cntry_cd) || ':' ||
       coalesce(nwis.state_cd, wqx.st_fips_cd) || ':' ||
       coalesce(nwis.county_cd, wqx.cnty_fips_cd) county_code,
       coalesce(nwis.county_nm, wqx.cnty_name) county_name
  from ${NWIS_SCHEMA_NAME}.county nwis
       full outer join (
                        select country."CNTRY_CD" cntry_cd,
                               to_char(state."ST_FIPS_CD", 'fm00') st_fips_cd,
                               county."CNTY_FIPS_CD" cnty_fips_cd,
                               county."CNTY_NAME" cnty_name
                          from ${WQX_SCHEMA_NAME}."COUNTRY" country
                               join ${WQX_SCHEMA_NAME}."STATE" state
                                 on country."CNTRY_UID" = state."CNTRY_UID"
                               join ${WQX_SCHEMA_NAME}."COUNTY" county
                                 on state."ST_UID" = county."ST_UID"
                       ) wqx
         on nwis.country_cd = wqx.cntry_cd and
            nwis.state_cd = wqx.st_fips_cd and
            nwis.county_cd = wqx.cnty_fips_cd