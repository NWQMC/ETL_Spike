insert /*+ append parallel(4) */
  into r_detect_qnt_lmt_swap_storet(data_source_id, data_source, station_id, site_id, event_date, activity, analytical_method,
                                    characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code,
                                    organization_name, project_id, assemblage_sampled_name, sample_tissue_taxonomic_name, activity_id,
                                    result_id, detection_limit_id, detection_limit, detection_limit_unit, detection_limit_desc)
select 3 data_source_id,
       'STORET' data_source,
       a.*
  from (select /*+ parallel(4) */
               station.station_id,
               station.site_id,
               result_no_source.event_date,
               result_no_source.activity,
               result_no_source.analytical_method,
               result_no_source.characteristic_name,
               result_no_source.characteristic_type,
               result_no_source.sample_media,
               station.organization,
               station.site_type,
               station.huc,
               station.governmental_unit_code,
               station.organization_name,
               result_no_source.project_id,
               null assemblage_sampled_name,
               result_no_source.sample_tissue_taxonomic_name,
               result_no_source.activity_id,
               result_no_source.result_id,
               result_no_source.result_id detection_limit_id,
               result_no_source.detection_limit,
               result_no_source.detection_limit_unit,
               result_no_source.detection_limit_desc
          from result_no_source
               join station_swap_storet station
                 on result_no_source.station_id + 10000000 = station.station_id) a