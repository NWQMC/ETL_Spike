update result_swap_storet
  set result_meas_qual_code=t2.result_meas_qual_code
  from
  (select
     activity, sample_media, characteristic_name, event_date, data_source_id, station_id, site_id,
     analytical_method, characteristic_type, site_type, huc, governmental_unit_code, geom,
     string_agg(result_meas_qual_code, ';' order by result_meas_qual_code) as result_meas_qual_code
     from result_swap_storet
     group by activity, sample_media, characteristic_name, event_date, data_source_id, station_id, site_id,
       analytical_method, characteristic_type, site_type, huc, governmental_unit_code, geom
  ) as t2
  where result_swap_storet.activity = t2.activity
    and result_swap_storet.sample_media = t2.sample_media
    and result_swap_storet.characteristic_name = t2.characteristic_name
    and result_swap_storet.event_date = t2.event_date
    and result_swap_storet.data_source_id = t2.data_source_id
    and result_swap_storet.station_id = t2.station_id
    and result_swap_storet.site_id = t2.site_id
    and result_swap_storet.analytical_method = t2.analytical_method
    and result_swap_storet.analytical_method = t2.analytical_method
    and result_swap_storet.characteristic_type = t2.characteristic_type
    and result_swap_storet.site_type = t2.site_type
    and result_swap_storet.huc = t2.huc
    and result_swap_storet.governmental_unit_code = t2.governmental_unit_code
    and result_swap_storet.geom = t2.geom;

delete from result_swap_storet t1
  using result_swap_storet t2
  where t1.result_id < t2.result_id
    and t1.activity = t2.activity
    and t1.sample_media = t2.sample_media
    and t1.characteristic_name = t2.characteristic_name
    and t1.event_date = t2.event_date
    and t1.data_source_id = t2.data_source_id
    and t1.station_id = t2.station_id
    and t1.site_id = t2.site_id
    and t1.analytical_method = t2.analytical_method
    and t1.analytical_method = t2.analytical_method
    and t1.characteristic_type = t2.characteristic_type
    and t1.site_type = t2.site_type
    and t1.huc = t2.huc
    and t1.governmental_unit_code = t2.governmental_unit_code
    and t1.geom = t2.geom
    and t1.result_meas_qual_code is not null
    and t2.result_meas_qual_code is not null;
