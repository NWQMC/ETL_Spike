create table if not exists ${WQX_SCHEMA_NAME}.result_taxon_feeding_group_aggregated
(res_uid                        numeric
,feeding_group_list             text
)
with (fillfactor = 100)
