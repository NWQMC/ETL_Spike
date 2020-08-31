insert
  into result_swap_storet (data_source_id, data_source, station_id, site_id, event_date, analytical_method, activity,
                           characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code, geom,
                           organization_name, activity_id, activity_type_code, activity_media_subdiv_name, activity_start_time,
                           act_start_time_zone, activity_stop_date, activity_stop_time, act_stop_time_zone, activity_depth,
                           activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,
                           activity_lower_depth, activity_lower_depth_unit, project_id, activity_conducting_org, activity_comment,
                           sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name, sample_collect_equip_name,
                           result_id, result_detection_condition_tx, sample_fraction_type, result_measure_value, result_unit,
                           result_meas_qual_code, result_value_status, statistic_type, result_value_type, weight_basis_type, duration_basis,
                           temperature_basis_level, particle_size, precision, result_comment, result_depth_meas_value,
                           result_depth_meas_unit_code, result_depth_alt_ref_pt_txt, sample_tissue_taxonomic_name,
                           analytical_procedure_id, analytical_procedure_source, analytical_method_name, lab_name,
                           analysis_start_date, lab_remark, detection_limit, detection_limit_unit, detection_limit_desc, analysis_prep_date_tx)
select 3 data_source_id,
       'STORET' data_source,
       a.*
  from (select station.station_id,
               station.site_id,
               result_no_source.event_date,
               result_no_source.analytical_method,
               result_no_source.activity,
               result_no_source.characteristic_name,
               result_no_source.characteristic_type,
               result_no_source.sample_media,
               station.organization,
               station.site_type,
               station.huc,
               station.governmental_unit_code,
               station.geom,
               station.organization_name,
               result_no_source.activity_id,
               result_no_source.activity_type_code,
               result_no_source.activity_media_subdiv_name,
               result_no_source.activity_start_time,
               result_no_source.act_start_time_zone,
               result_no_source.activity_stop_date,
               result_no_source.activity_stop_time,
               result_no_source.act_stop_time_zone,
               result_no_source.activity_depth,
               result_no_source.activity_depth_unit,
               result_no_source.activity_depth_ref_point,
               result_no_source.activity_upper_depth,
               result_no_source.activity_upper_depth_unit,
               result_no_source.activity_lower_depth,
               result_no_source.activity_lower_depth_unit,
               result_no_source.project_id,
               result_no_source.activity_conducting_org,
               result_no_source.activity_comment,
               result_no_source.sample_collect_method_id,
               result_no_source.sample_collect_method_ctx,
               result_no_source.sample_collect_method_name,
               result_no_source.sample_collect_equip_name,
               result_no_source.result_id,
               result_no_source.result_detection_condition_tx,
               result_no_source.sample_fraction_type,
               result_no_source.result_measure_value,
               result_no_source.result_unit,
               result_no_source.result_meas_qual_code,
               result_no_source.result_value_status,
               result_no_source.statistic_type,
               result_no_source.result_value_type,
               result_no_source.weight_basis_type,
               result_no_source.duration_basis,
               result_no_source.temperature_basis_level,
               result_no_source.particle_size,
               result_no_source.precision,
               result_no_source.result_comment,
               result_no_source.result_depth_meas_value,
               result_no_source.result_depth_meas_unit_code,
               result_no_source.result_depth_alt_ref_pt_txt,
               result_no_source.sample_tissue_taxonomic_name,
               result_no_source.analytical_procedure_id,
               result_no_source.analytical_procedure_source,
               result_no_source.analytical_method_name,
               result_no_source.lab_name,
               result_no_source.analysis_date_time,
               result_no_source.lab_remark,
               result_no_source.detection_limit,
               result_no_source.detection_limit_unit,
               result_no_source.detection_limit_desc,
               result_no_source.analysis_prep_date_tx
          from storetw.result_no_source
               join station_swap_storet station
                 on result_no_source.station_id + 10000000 = station.station_id) a;

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
