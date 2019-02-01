insert into organization_sum_swap_storet
    (data_source_id, data_source, organization_id, organization, organization_name, organization_url,
     all_time_last_result, all_time_site_count, all_time_activity_count,
     five_year_last_result, five_year_site_count, five_year_activity_count,
     current_year_last_result, current_year_site_count, current_year_activity_count,
     all_time_summary, five_year_summary, current_year_summary, organization_type)
select /*+ leading(org_data org_sum) full(org_data) use_hash(org_data) full(org_sum) use_hash(org_sum) */
       org_data.data_source_id,
       org_data.data_source,
       org_data.organization_id,
       org_data.organization,
       org_data.organization_name,
       org_data.organization_url,
       org_sum.event_date_all_time all_time_last_result,
       nvl2(org_sum.event_date_all_time, org_sum.site_count_all_time, null) all_time_site_count,
       nvl2(org_sum.event_date_all_time, org_sum.activity_count_all_time, null) all_time_activity_count,
       org_sum.event_date_five_year five_year_last_result,
       nvl2(org_sum.event_date_five_year, org_sum.site_count_five_year, null) five_year_site_count,
       nvl2(org_sum.event_date_five_year, org_sum.activity_count_five_year, null) five_year_activity_count,
       org_sum.event_date_current_year current_year_last_result,
       nvl2(org_sum.event_date_current_year, org_sum.site_count_current_year, null) current_year_site_count,
       nvl2(org_sum.event_date_current_year, org_sum.activity_count_current_year, null) current_year_activity_count,
       null all_time_summary,
       null five_year_summary,
       null current_year_summary,
       org_data.organization_type
  from (select data_source_id,
               data_source,
               organization_id,
               organization,
               organization_name,
               '/provider/' || data_source || '/' || organization organization_url,
               organization_type
          from org_data_swap_storet) org_data,
       org_sum_temp_storet org_sum
 where org_data.data_source_id = org_sum.data_source_id(+) and
       org_data.organization   = org_sum.organization(+)
