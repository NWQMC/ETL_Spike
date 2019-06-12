create extension tablefunc;

create role storet_owner with login password 'changeMe';
grant storet_owner to postgres;
create schema wqx authorization storet_owner;
alter default privileges in schema wqx grant select on tables to wqp_core;
grant usage on schema wqx to wqp_core;

create or replace view wqx.country as
select distinct
       "CNTRY_UID" cntry_uid,
       "CNTRY_CD" cntry_cd,
       "CNTRY_NAME" cntry_name,
       "CNTRY_LAST_CHANGE_DATE" cntry_last_change_date,
       "USR_UID_LAST_CHANGE" usr_uid_last_change
  from wqx."COUNTRY";

create or replace view wqx.state as
select distinct
       "ST_UID" st_uid,
       "CNTRY_UID" cntry_uid,
       "ST_CD" st_cd,
       "ST_NAME" st_name,
       "ST_FIPS_CD" st_fips_cd,
       "ST_LAST_CHANGE_DATE" st_last_change_date,
       "ER_UID" er_uid,
       "USR_UID_LAST_CHANGE" usr_uid_last_change
  from wqx."STATE";

create or replace view wqx.county as
select distinct
       "CNTY_UID" cnty_uid,
       "ST_UID" st_uid,
       "CNTY_NAME" cnty_name,
       "CNTY_FIPS_CD" cnty_fips_cd,
       "CNTY_LAST_CHANGE_DATE" cnty_last_change_date,
       "USR_UID_LAST_CHANGE" usr_uid_last_change
  from wqx."COUNTY";

create or replace view wqx.full_country (country_code, country_name) as
select distinct
       coalesce(nwis.country_cd, wqx."CNTRY_CD") country_code,
       coalesce(nwis.country_nm, wqx."CNTRY_NAME") country_name
  from nwis.country nwis
       full outer join wqx."COUNTRY" wqx
         on nwis.country_cd = wqx."CNTRY_CD";
         
create or replace view wqx.full_county (county_code, county_name) as
select distinct
       coalesce(nwis.country_cd, wqx.cntry_cd) || ':' ||
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
select distinct
       coalesce(nwis.country_cd, wqx.cntry_cd) || ':' ||
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

create unlogged table if not exists wqx.monitoring_location_local
(monitoring_location_source     character varying (7)
,station_id                     numeric
,site_id                        text
,latitude                       numeric
,longitude                      numeric
,hrdat_uid                      numeric
,huc                            character varying (12)
,cntry_cd                       character varying (2)
,st_fips_cd                     character varying (2)
,cnty_fips_cd                   character varying (3)
,calculated_huc_12              character varying (12)
,calculated_fips                character varying (5)
,geom                           geometry(point,4269)
,constraint wqx_monitoring_location_local_pk primary key (monitoring_location_source, station_id)
,constraint wqx_monitoring_location_local_uk unique (monitoring_location_source, site_id)
);
-- wqp_core needs update/select/delete? on ^

create unlogged table if not exists wqx.site_type_conversion
(mltyp_uid                      numeric
,mltyp_name                     character varying (45)
,station_group_type             character varying (256)
,constraint site_type_conversion_pk
  primary key (mltyp_uid)
)
with (fillfactor = 100);
--LOAD TABLE ^ WITH DATA

create unlogged table if not exists wqx.activity_project_aggregated
(act_uid                        numeric
,project_id_list                text
,project_name_list              text
)
with (fillfactor = 100);
-- wqp_core needs all? on ^

create unlogged table if not exists wqx.activity_conducting_org_aggregated
(act_uid                        numeric
,acorg_name_list                text
)
with (fillfactor = 100);
-- wqp_core needs all? on ^

create unlogged table if not exists wqx.activity_metric_sum
(act_uid                        numeric
,activity_metric_count          numeric
)
with (fillfactor = 100);
-- wqp_core needs all? on ^

create unlogged table if not exists wqx.nemi_wqp_to_epa_crosswalk
(wqp_source                     character varying (6)
,analytical_procedure_source    character varying (256)
,analytical_procedure_id        character varying (256)
,source_method_identifier       character varying (30)
,method_id                      numeric
,method_source                  character varying (20)
,method_type                    character varying (100)
,nemi_url                       text
)
with (fillfactor = 100);
-- wqp_core needs all? on ^

create unlogged table if not exists wqx.analytical_method_plus_nemi
(anlmth_uid                     numeric
,anlmth_id                      character varying(20)
,amctx_cd                       character varying(30)
,anlmth_name                    character varying(120)
,anlmth_url                     character varying(256)
,anlmth_qual_type               character varying(25)
,nemi_url                       text
)
with (fillfactor = 100);
-- wqp_core needs all? on ^

create unlogged table if not exists wqx.r_detect_qnt_lmt
(res_uid                        numeric
,rdqlmt_uid                     numeric
,rdqlmt_measure                 character varying(12)
,msunt_cd                       character varying(12)
,dqltyp_uid                     numeric
,dqltyp_name                    character varying(35)
)
with (fillfactor = 100);
-- wqp_core needs all? on ^

create unlogged table if not exists wqx.result_taxon_habit_aggregated
(res_uid                        numeric
,habit_name_list                text
)
with (fillfactor = 100);
-- wqp_core needs all? on ^

create unlogged table if not exists wqx.result_taxon_feeding_group_aggregated
(res_uid                        numeric
,feeding_group_list             text
)
with (fillfactor = 100);
-- wqp_core needs all? on ^

create unlogged table if not exists wqx.result_lab_sample_prep_sum
(res_uid                        numeric
,result_lab_sample_prep_count   numeric
)
with (fillfactor = 100);
-- wqp_core needs all? on ^

create unlogged table if not exists wqx.result_frequency_class_aggregated
(res_uid                        numeric
,frequency_class_code_1         text
,frequency_class_unit_1         text
,frequency_class_lower_bound_1  text
,frequency_class_upper_bound_1  text
,frequency_class_code_2         text
,frequency_class_unit_2         text
,frequency_class_lower_bound_2  text
,frequency_class_upper_bound_2  text
,frequency_class_code_3         text
,frequency_class_unit_3         text
,frequency_class_lower_bound_3  text
,frequency_class_upper_bound_3  text
)
with (fillfactor = 100);
-- wqp_core needs all? on ^

create unlogged table if not exists wqx.detection_quant_limit
(res_uid                        numeric
,rdqlmt_measure                 character varying(12)
,msunt_cd                       character varying(12)
,dqltyp_name                    character varying(35)
)
with (fillfactor = 100);
-- wqp_core needs all? on ^

create unlogged table if not exists wqx.dql_hierarchy
(hierarchy_value                numeric
,dqltyp_uid                     numeric
,dqltyp_name                    character varying(35)
,constraint dql_hierarchy_pk primary key (hierarchy_value)
,constraint dql_hierarchy_ak unique (dqltyp_uid)
)
with (fillfactor = 100);
-- wqp_core needs all? on ^
--LOAD TABLE ^ WITH DATA


