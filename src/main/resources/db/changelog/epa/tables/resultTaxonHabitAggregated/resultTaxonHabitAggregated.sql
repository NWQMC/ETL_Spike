create table if not exists ${WQX_SCHEMA_NAME}.result_taxon_habit_aggregated
(res_uid                        numeric
,habit_name_list                text
)
with (fillfactor = 100)
