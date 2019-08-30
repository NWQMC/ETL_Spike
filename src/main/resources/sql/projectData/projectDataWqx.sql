insert
  into project_data_swap_storet (data_source_id,
                                 project_id,
                                 data_source,
                                 organization,
                                 organization_name,
                                 project_identifier,
                                 project_name,
                                 description,
                                 sampling_design_type_code,
                                 qapp_approved_indicator,
                                 qapp_approval_agency_name,
--                                 project_file_url,
                                 monitoring_location_weight_url
                                )
select 3 data_source_id,
       project."PRJ_UID" project_id,
       'STORET' data_source,
       organization."ORG_ID" organization,
       organization."ORG_NAME" organization_name,
       project."PRJ_ID" project_identifier,
       project."PRJ_NAME" project_name,
       project."PRJ_DESC" description,
       sampling_design_type."SDTYP_DESC" sampling_design_type_code,
       project."PRJ_QAPP_APPROVED_YN" qapp_approved_indicator,
       project."PRJ_QAPP_APPROVAL_AGENCY_NAME" qapp_approval_agency_name,
--       case 
--         when attached_object.has_blob is null
--           then null
--         else
--           '/providers/STORET/organizations/' || encode_uri_component(organization."ORG_ID") ||
--             '/projects/' || encode_uri_component(project."PRJ_ID") ||
--             '/files'
--         else null
--       end project_file_url,
       case
         when monitoring_location_weight.has_weight is null
           then null
         else
           '/providers/STORET/organizations/' || encode_uri_component(organization."ORG_ID") ||
             '/projects/' || encode_uri_component(project."PRJ_ID") ||
             '/projectMonitoringLocationWeightings'
       end monitoring_location_weight_url
  from wqx_dump."PROJECT" project
       join wqx_dump."ORGANIZATION" organization
         on project."ORG_UID" = organization."ORG_UID"
       left join wqx_dump."SAMPLING_DESIGN_TYPE" sampling_design_type
         on project."SDTYP_UID" = sampling_design_type."SDTYP_UID"
--       left join (select org_uid, ref_uid, count(*) has_blob
--                    from wqx.attached_object
--                   where 1 = tbl_uid
--                     group by org_uid, ref_uid) attached_object
--         on project.org_uid = attached_object.org_uid and
--            project.prj_uid = attached_object.ref_uid
       left join (select "PRJ_UID" prj_uid, count(*) has_weight
                    from wqx_dump."MONITORING_LOCATION_WEIGHT" monitoring_location_weight
                      group by prj_uid) monitoring_location_weight
         on project."PRJ_UID" = monitoring_location_weight.prj_uid
