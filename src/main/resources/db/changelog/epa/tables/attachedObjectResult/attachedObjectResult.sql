create unlogged table if not exists ${WQX_SCHEMA_NAME}.attached_object_result
(org_uid             numeric
,ref_uid             numeric
,result_object_name  text
,result_object_type  text
)
with (fillfactor = 100)
