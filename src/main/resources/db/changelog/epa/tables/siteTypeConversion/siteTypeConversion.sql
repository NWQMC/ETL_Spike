create unlogged table if not exists ${WQX_SCHEMA_NAME}.site_type_conversion
(mltyp_uid                      numeric
,mltyp_name                     character varying (45)
,station_group_type             character varying (256)
,constraint site_type_conversion_pk
  primary key (mltyp_uid)
)
with (fillfactor = 100)
