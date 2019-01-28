insert  into 
ORGANIZATION_SUM_SWAP_STORET(data_source_id, data_source, organization_id, organization, organization_name, organization_url,
                          all_time_last_result, all_time_site_count, all_time_activity_count,
                          five_year_last_result, five_year_site_count, five_year_activity_count,
                          current_year_last_result, current_year_site_count, current_year_activity_count,
                          all_time_summary, five_year_summary, current_year_summary, organization_type)
select /*+ leading(org_data org_sum year_summary) full(org_data) use_hash(org_data) full(org_sum) use_hash(org_sum) full(year_summary) use_hash(year_summary) */
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
   year_summary.all_time_summary,
   year_summary.five_year_summary,
   year_summary.current_year_summary,
   org_data.organization_type
from 
   (select /*+ noparallel(Z) */ 
       data_source_id,    
       data_source,       
       organization_id,   
       organization,      
       organization_name, 
       '/provider/' || data_source || '/' || organization organization_url,
       organization_type  
    from 
       org_data_swap_STORET Z) org_data,
   (select /*+ full(R) noparallel(R) */
          data_source_id, 
          organization,   
          max(nvl(last_updated, event_date)) event_date_all_time,     
          count(distinct site_id) site_count_all_time,   
          count(distinct activity_id) activity_count_all_time,
          max(case when event_date >= add_months(trunc(sysdate, 'yyyy'), - 48) then nvl(last_updated, event_date) else null end) event_date_five_year,
          count(distinct case when event_date >= add_months(trunc(sysdate, 'yyyy'), - 48) then site_id else null end) site_count_five_year,
          count(distinct case when event_date >= add_months(trunc(sysdate, 'yyyy'), - 48) then activity_id else null end) activity_count_five_year,
          max(case when event_date >= trunc(sysdate, 'yyyy') then nvl(last_updated, event_date) else null end) event_date_current_year,
          count(distinct case when event_date >= trunc(sysdate, 'yyyy') then site_id else null end) site_count_current_year,
          count(distinct case when event_date >= trunc(sysdate, 'yyyy') then activity_id else null end) activity_count_current_year
       from 
          result_swap_STORET R 
       group by data_source_id, organization) org_sum,
   (select /*+ noparallel(W) */  
       data_source_id,
       organization,  
       to_clob('[') || rtrim(clobagg(case when years_window = 1 then to_clob(year_data || ',') else null end), ',') || to_clob(']') current_year_summary,    
       to_clob('[') || rtrim(clobagg(case when years_window < 6 then to_clob(year_data || ',') else null end), ',') || to_clob(']') five_year_summary,  
       to_clob('[') || rtrim(clobagg(year_data || ','), ', ') ||  to_clob(']') all_time_summary    
    from 
       (select /*+ noparallel(org_year_agg) */ 
           data_source_id,
           organization,       
           years_window,       
           the_year,   
           '{' || year_metadata || ', ' || group_result_counts || ', ' || name_result_counts || '}' year_data    
        from 
           (select /*+ noparallel(M) */ 
               data_source_id,    
               organization,   
               the_year,  
               '"characteristicGroupResultCount":{' ||   
               listagg(case when grouping_id = 1 then '"' || characteristic_type || '": ' || total_results else null end, ',')
                        within group (order by characteristic_type) || '}' group_result_counts,  
               case  
                  when the_year >= to_char(sysdate, 'yyyy') THEN 1     
                  when the_year >= to_char(add_months(trunc(sysdate, 'yyyy'), - 48), 'yyyy') then 5       
                  else null
               end years_window,       
               '"year": ' || the_year ||  ', "start": "01-01-' || the_year || '", "end": "12-31-' || the_year ||    
               '", "activityCount": ' || min(case when grouping_id = 3 then total_activities else null end) ||
               ', "resultCount": ' || min(case when grouping_id = 3 then total_results else null end) ||  
               ', "monitoringLocationsSampled": ' || min(case when grouping_id = 3 then total_monitoring_locations else null end) year_metadata,
               to_clob('"characteristicNameSummary":[') ||    
                   rtrim(clobagg(case when grouping_id = 0  
                                      then '{"characteristicName": "' || CHARACTERISTIC_NAME || '",' || '"characteristicType": "' || CHARACTERISTIC_TYPE || '",' ||    
                                                                '"activityCount":' || total_activities || ',' ||  
                                                                '"resultCount":' || total_results || ',' ||       
                                                                '"monitoringLocationCount":' || total_monitoring_locations || '}, ' 
                                      else null end), ', ') || to_clob(']') name_result_counts  
                from 
                   org_grouping_swap_STORET M  
                where 
                   grouping_id in (0, 1, 3)
                group by 
                   data_source_id, organization, the_year   
            ) org_year_agg
        ) W
        group by data_source_id, organization       
    ) year_summary
   where
      org_data.data_source_id = org_sum.data_source_id(+) and
      org_data.organization   = org_sum.organization(+) and
      org_data.data_source_id = year_summary.data_source_id(+) and
      org_data.organization   = year_summary.organization(+)
