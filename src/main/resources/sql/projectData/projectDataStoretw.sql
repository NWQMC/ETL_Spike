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
                                 qapp_approval_agency_name
                                )
select 3 data_source_id,
       project_id,
       'STORET' data_source,
       organization,
       organization_name,
       project_identifier,
       project_name,
       description,
       sampling_design_type_code,
       qapp_approved_indicator,
       qapp_approval_agency_name
  from storetw.project_data_no_source
