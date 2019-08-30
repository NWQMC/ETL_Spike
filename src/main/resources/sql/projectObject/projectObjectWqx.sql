insert
  into WQP.project_object_swap_storet(data_source_id,
                                  object_id,
                                  data_source,
                                  organization,
                                  project_identifier,
                                  object_name,
                                  object_type,
                                  object_content)
select '3' data_source_id,
       attached_object."ATOBJ_UID" object_id,
       'STORET' data_source,
       organization."ORG_ID" organization,
       project."PRJ_ID" project_identifier,
       attached_object."ATOBJ_FILE_NAME" object_name,
       attached_object."ATOBJ_TYPE" object_type,
       attached_object."ATOBJ_CONTENT" object_content
  from wqx_dump."ATTACHED_OBJECT" attached_object
       join wqx_dump."ORGANIZATION" organization
         on attached_object."ORG_UID" = organization."ORG_UID"
       join wqx_dump."PROJECT" project
         on attached_object."REF_UID" = project."PRJ_UID"
 where attached_object."TBL_UID" = 1