create table year_sum_temp_storet as
select /*+ all_rows full(W) noparallel(W) */
       data_source_id,
       organization,
       to_clob('[') || rtrim(clobagg(case when years_window = 1 then to_clob(year_data || ',') else null end), ',') || to_clob(']') current_year_summary,
       to_clob('[') || rtrim(clobagg(case when years_window < 6 then to_clob(year_data || ',') else null end), ',') || to_clob(']') five_year_summary,
       to_clob('[') || rtrim(clobagg(year_data || ','), ', ') ||  to_clob(']') all_time_summary
  from (select /*+ full(org_year_agg) noparallel(org_year_agg) */
	       data_source_id,
	       organization,
	       years_window,
	       the_year,
	       '{' || year_metadata || ', ' || group_result_counts || ', ' || name_result_counts || '}' year_data
	  from (select /*+ full(M) noparallel(M) */
		       data_source_id,
		       organization,
		       the_year,
		       '"characteristicGroupResultCount":{' ||
		       listagg(case when grouping_id = 1 then '"' || characteristic_type || '": ' || total_results else null end, ', ')
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
			       rtrim(clobagg(case
						when grouping_id = 0
						   then '{"characteristicName": "' || CHARACTERISTIC_NAME || '",' || '"characteristicType": "' || CHARACTERISTIC_TYPE || '",' ||
							'"activityCount":' || total_activities || ',' ||
							'"resultCount":' || total_results || ',' ||
							'"monitoringLocationCount":' || total_monitoring_locations || '}, '
						else null end), ', ') ||
			       to_clob(']') name_result_counts
		  from org_grouping_swap_STORET M
		 where grouping_id in (0, 1, 3)
		    group by data_source_id, organization, the_year
		) org_year_agg
	) W
    group by data_source_id, organization