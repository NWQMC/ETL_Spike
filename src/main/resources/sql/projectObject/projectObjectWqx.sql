insert /*+ append parallel(4) */
  into project_object_swap_storet(data_source_id,
                                  object_id,
                                  data_source,
                                  organization,
                                  project_identifier,
                                  object_name,
                                  object_type,
                                  object_content)
select '3',
       attached_object.atobj_uid,
       'STORET',
       organization.org_id organization,
       project.prj_id project_identifier,
       attached_object.atobj_file_name,
       attached_object.atobj_type,
       attached_object.atobj_content
  from wqx.attached_object
       join wqx.organization
         on attached_object.org_uid = organization.org_uid
       join wqx.project
         on attached_object.ref_uid = project.prj_uid
 where attached_object.tbl_uid = 1