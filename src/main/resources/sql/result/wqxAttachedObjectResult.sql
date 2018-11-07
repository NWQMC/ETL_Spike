insert /*+ append parallel(4) */
  into wqx_attached_object_result (org_uid, ref_uid, result_object_name, result_object_type)
select /*+ parallel(4) */
       org_uid,
       ref_uid,
       listagg(atobj_file_name, ';') within group (order by rownum) result_object_name,
       listagg(atobj_type, ';') within group (order by rownum) result_object_type
  from wqx.attached_object
 where tbl_uid = 5
    group by org_uid, ref_uid