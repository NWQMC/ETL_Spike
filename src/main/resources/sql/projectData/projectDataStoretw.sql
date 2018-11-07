insert /*+ append parallel(4) */
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
                                 qapp_approval_agency_name
                                )
select 3 data_source_id,
       di_project.pk_isn + 100000000000 project_id,
       'STORET' data_source,
       di_org.organization_id organization,
       di_org.organization_name,
       di_project.project_cd project_identifier,
       di_project.project_name,
       di_project.project_description,
       di_project.sampling_design_type_cd,
       di_project.qa_approved qapp_approved_indicator,
       di_project.qa_approval_agency qapp_approval_agency_name
  from storetw.di_project
       join storetw.di_org
         on di_project.fk_org = di_org.pk_isn
 where di_project.tsmproj_org_id not in (select org_id from wqp_core.storetw_transition) and
       lnnvl(di_project.source_system = 'WQX')