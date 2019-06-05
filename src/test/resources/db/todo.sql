create role storet_owner with login password 'changeMe';
grant storet_owner to postgres;
create schema wqx authorization storet_owner;

alter default privileges in schema wqx grant select on tables to wqp_core;
grant usage on schema wqx to wqp_core;

create or replace view wqx.full_country (country_code, country_name) as
select coalesce(nwis.country_cd, wqx."CNTRY_CD") country_code,
       coalesce(nwis.country_nm, wqx."CNTRY_NAME") country_name
  from nwis.country nwis
       full outer join wqx."COUNTRY" wqx
         on nwis.country_cd = wqx."CNTRY_CD";
         
create or replace view wqx.full_county (county_code, county_name) as
select coalesce(nwis.country_cd, wqx.cntry_cd) || ':' ||
       coalesce(nwis.state_cd, wqx.st_fips_cd) || ':' ||
       coalesce(nwis.county_cd, wqx.cnty_fips_cd) county_code,
       coalesce(nwis.county_nm, wqx.cnty_name) county_name
  from nwis.county nwis
       full outer join (
                        select country."CNTRY_CD" cntry_cd,
                               to_char(state."ST_FIPS_CD", 'fm00') st_fips_cd,
                               county."CNTY_FIPS_CD" cnty_fips_cd,
                               county."CNTY_NAME" cnty_name
                          from wqx."COUNTRY" country
                               join wqx."STATE" state
                                 on country."CNTRY_UID" = state."CNTRY_UID"
                               join wqx."COUNTY" county
                                 on state."ST_UID" = county."ST_UID"
                       ) wqx
         on nwis.country_cd = wqx.cntry_cd and
            nwis.state_cd = wqx.st_fips_cd and
            nwis.county_cd = wqx.cnty_fips_cd;
            
create or replace view wqx.full_state (state_code, state_name) as
select coalesce(nwis.country_cd, wqx.cntry_cd) || ':' ||
       coalesce(nwis.state_cd, wqx.st_fips_cd) state_code,
       coalesce(nwis.state_nm, wqx.st_name) state_name
  from nwis.state nwis
       full outer join (
                        select country."CNTRY_CD" cntry_cd,
                               to_char(state."ST_FIPS_CD", 'fm00') st_fips_cd,
                               state."ST_NAME" st_name
                          from wqx."COUNTRY" country
                               join wqx."STATE" state
                                 on country."CNTRY_UID" = state."CNTRY_UID"
                       ) wqx
         on nwis.country_cd = wqx.cntry_cd and
            nwis.state_cd = wqx.st_fips_cd;