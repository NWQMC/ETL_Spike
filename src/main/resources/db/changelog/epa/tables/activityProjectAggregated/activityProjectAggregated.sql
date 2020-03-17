create unlogged table if not exists ${WQX_SCHEMA_NAME}.activity_project_aggregated
(act_uid                        numeric
,project_id_list                text
,project_name_list              text
)
with (fillfactor = 100)
