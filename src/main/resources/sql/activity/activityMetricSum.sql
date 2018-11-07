insert /*+ append parallel(4) */ into wqx_activity_metric_sum (act_uid, activity_metric_count)
select /*+ parallel(4) */
       act_uid,
       count(*) activity_metric_count
  from wqx.activity_metric
    group by act_uid