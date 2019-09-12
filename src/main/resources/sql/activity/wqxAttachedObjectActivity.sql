insert into wqx.attached_object_activity (org_uid, ref_uid, activity_object_name, activity_object_type)
select "ORG_UID",
       "REF_UID",
       string_agg("ATOBJ_FILE_NAME", ';' order by "ATOBJ_UID") activity_object_name,
       string_agg("ATOBJ_TYPE", ';' order by "ATOBJ_UID") activity_object_type
  from wqx_dump."ATTACHED_OBJECT" attached_object
 where "TBL_UID" = 3
    group by "ORG_UID", "REF_UID"