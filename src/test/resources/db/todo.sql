create role storet_owner with login password 'changeMe';
grant storet_owner to postgres;
create schema wqx authorization storet_owner;
create or replace view wqx.full_country (country_code, country_name) as
select country_cd,
       country_nm
  from nwis.country;
create or replace view wqx.full_county (county_code, county_name) as
select country_cd || ':' || state_cd || ':' || county_cd,
       county_nm
  from nwis.county;
create or replace view wqx.full_state (state_code, state_name) as
select country_cd || ':' || state_cd,
       state_nm
  from nwis.state;
alter default privileges in schema wqx grant select on tables to wqp_core;
grant usage on schema wqx to wqp_core;