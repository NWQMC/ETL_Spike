insert into wqx.activity_metric_sum (act_uid, activity_metric_count)
select "ACT_UID" act_uid,
       count(*) activity_metric_count
  from wqx_dump."ACTIVITY_METRIC" activity_metric
    group by "ACT_UID"
