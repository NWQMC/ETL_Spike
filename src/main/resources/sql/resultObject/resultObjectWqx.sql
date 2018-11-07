insert /*+ append parallel(4) */
  into result_object_swap_storet (data_source_id,
                                  object_id,
                                  data_source,
                                  organization,
                                  activity_id,
                                  activity,
                                  result_id,
                                  object_name,
                                  object_type,
                                  object_content)
select '3' data_source_id,
       attached_object.atobj_uid object_id,
       'STORET' data_source,
       result_swap_storet.organization,
       result_swap_storet.activity_id,
       result_swap_storet.activity,
       attached_object.ref_uid,
       attached_object.atobj_file_name object_name,
       attached_object.atobj_type object_type,
       attached_object.atobj_content object_content
  from wqx.attached_object
       join result_swap_storet
         on attached_object.ref_uid = result_swap_storet.result_id
 where attached_object.tbl_uid = 5