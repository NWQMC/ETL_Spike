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
       attached_object."ATOBJ_UID" object_id,
       'STORET' data_source,
       result_swap_storet.organization,
       result_swap_storet.activity_id,
       result_swap_storet.activity,
       result_swap_storet.result_id,
       attached_object."ATOBJ_FILE_NAME" object_name,
       attached_object."ATOBJ_TYPE" object_type,
       attached_object."ATOBJ_CONTENT" object_content
  from wqx_dump."ATTACHED_OBJECT" attached_object
       join result_swap_storet
         on attached_object."REF_UID" = result_swap_storet.result_id
 where attached_object."TBL_UID" = 5