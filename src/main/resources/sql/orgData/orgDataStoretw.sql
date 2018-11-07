insert /*+ append parallel(4) */
  into org_data_swap_storet (data_source_id, data_source, organization_id, organization, organization_name,
                             organization_description, organization_type)
select /*+ parallel(4) */
       3 data_source_id,
       'STORET' data_source,
       pk_isn + 10000000  organization_id,
       organization_id organization,
       organization_name,
       organization_description,
       organization_type
  from storetw.di_org
 where source_system is null and
       organization_id not in(select org_id from wqp_core.storetw_transition)