create unlogged table if not exists ${WQX_SCHEMA_NAME}.monitoring_location_local
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
,constraint monitoring_location_local_pk primary key (monitoring_location_source, station_id)
,constraint monitoring_location_local_uk unique (site_id)
)
