insert
  into r_detect_qnt_lmt_swap_storet(data_source_id, data_source, station_id, site_id, event_date, activity, analytical_method,
                                    characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code, geom,
                                    organization_name, project_id, assemblage_sampled_name, sample_tissue_taxonomic_name, activity_id,
                                    result_id, detection_limit_id, detection_limit, detection_limit_unit, detection_limit_desc)
select result_swap_storet.data_source_id,
       result_swap_storet.data_source,
       result_swap_storet.station_id,
       result_swap_storet.site_id,
       result_swap_storet.event_date,
       result_swap_storet.activity,
       result_swap_storet.analytical_method,
       result_swap_storet.characteristic_name,
       result_swap_storet.characteristic_type,
       result_swap_storet.sample_media,
       result_swap_storet.organization,
       result_swap_storet.site_type,
       result_swap_storet.huc,
       result_swap_storet.governmental_unit_code,
       result_swap_storet.geom,
       result_swap_storet.organization_name,
       result_swap_storet.project_id,
       result_swap_storet.assemblage_sampled_name,
       result_swap_storet.sample_tissue_taxonomic_name,
       result_swap_storet.activity_id,
       result_swap_storet.result_id,
       r_detect_qnt_lmt.rdqlmt_uid,
       r_detect_qnt_lmt.rdqlmt_measure,
       r_detect_qnt_lmt.msunt_cd,
       r_detect_qnt_lmt.dqltyp_name
  from wqx.r_detect_qnt_lmt
       join result_swap_storet
         on r_detect_qnt_lmt.res_uid = result_swap_storet.result_id