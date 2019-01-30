create table org_sum_temp_storet as
select /*+ full(R) parallel(4,R) */
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
  from result_swap_STORET R 
    group by data_source_id, organization