insert /*+ append parallel(4) */ into wqx_attached_object_activity (org_uid, ref_uid, activity_object_name, activity_object_type)
select /*+ parallel(4) */
       org_uid,
       ref_uid,
       listagg(atobj_file_name, ';') within group (order by rownum) activity_object_name,
       listagg(atobj_type, ';') within group (order by rownum) activity_object_type
  from wqx.attached_object
 where tbl_uid = 3
    group by org_uid, ref_uid