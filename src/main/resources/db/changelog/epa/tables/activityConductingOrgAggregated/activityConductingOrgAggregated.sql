create table if not exists ${WQX_SCHEMA_NAME}.activity_conducting_org_aggregated
(act_uid                        numeric
,acorg_name_list                text
)
with (fillfactor = 100)
