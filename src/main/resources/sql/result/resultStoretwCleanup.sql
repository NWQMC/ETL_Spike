update result_swap_storet
  set result_meas_qual_code=t2.result_meas_qual_code
  from
  (select
     activity, sample_media, characteristic_name, event_date,
     string_agg(result_meas_qual_code, ';' order by result_meas_qual_code) as result_meas_qual_code,
	 count(*)
     from (select * from result_swap_storet where result_meas_qual_code is not null) as foo
     group by activity, sample_media, characteristic_name, event_date
	 having count(*) > 1
  ) as t2
  where
    result_swap_storet.result_meas_qual_code is not null
    and result_swap_storet.activity = t2.activity
    and result_swap_storet.sample_media = t2.sample_media
    and result_swap_storet.characteristic_name = t2.characteristic_name
    and result_swap_storet.event_date = t2.event_date;

delete from result_swap_storet t1
  using result_swap_storet t2
  where t1.ctid < t2.ctid
    and t1.activity = t2.activity
    and t1.sample_media = t2.sample_media
    and t1.characteristic_name = t2.characteristic_name
    and t1.event_date = t2.event_date
    and t1.result_measure_qual_code is not null
    and t1.result_meas_qual_code = t2.result_meas_qual_code;



