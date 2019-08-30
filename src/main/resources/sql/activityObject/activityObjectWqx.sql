insert /*+ append parallel(4) */
  into activity_object_swap_storet (data_source_id,
                                    object_id,
                                    data_source,
                                    activity_id,
                                    organization,
                                    activity,
                                    object_name,
                                    object_type,
                                    object_content)
select '3' data_source_id,
       attached_object."ATOBJ_UID" object_id,
       'STORET' data_source,
       attached_object."REF_UID" activity_id,
       activity_swap_storet.organization,
       activity_swap_storet.activity,
       attached_object."ATOBJ_FILE_NAME" object_name,
       attached_object."ATOBJ_TYPE" object_type,
       attached_object."ATOBJ_CONTENT" object_content
  from wqx_dump."ATTACHED_OBJECT" attached_object
       join activity_swap_storet
         on attached_object."REF_UID" = activity_swap_storet.activity_id
 where attached_object."TBL_UID" = 3