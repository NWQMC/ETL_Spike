create unlogged table if not exists ${WQX_SCHEMA_NAME}.hrdat_to_srid
(hrdat_uid                      numeric
,srid                           integer
)
with (fillfactor = 100)
