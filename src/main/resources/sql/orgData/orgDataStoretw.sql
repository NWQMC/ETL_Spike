insert
  into org_data_swap_storet (data_source_id, data_source, organization_id, organization, organization_name,
                             organization_description, organization_type)
select 3 data_source_id,
       'STORET' data_source,
       organization_id,
       organization,
       organization_name,
       organization_description,
       organization_type
  from storetw.org_data_no_source
