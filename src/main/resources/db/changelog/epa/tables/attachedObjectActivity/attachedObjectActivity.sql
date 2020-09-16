create table if not exists ${WQX_SCHEMA_NAME}.attached_object_activity
(org_uid               numeric
,ref_uid               numeric
,activity_object_name  text
,activity_object_type  text
)
with (fillfactor = 100)
