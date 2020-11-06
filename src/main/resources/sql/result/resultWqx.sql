insert
  into result_swap_storet (data_source_id, data_source, station_id, site_id, event_date, analytical_method, activity,
                           characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code, geom,
                           organization_name, activity_id, activity_type_code, activity_media_subdiv_name, activity_start_time,
                           act_start_time_zone, activity_stop_date, activity_stop_time, act_stop_time_zone, activity_relative_depth_name, activity_depth,
                           activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,
                           activity_lower_depth, activity_lower_depth_unit, project_id, activity_conducting_org, activity_comment,
                           activity_latitude, activity_longitude, activity_source_map_scale, act_horizontal_accuracy, act_horizontal_accuracy_unit,
                           act_horizontal_collect_method, act_horizontal_datum_name, assemblage_sampled_name, act_collection_duration, act_collection_duration_unit,
                           act_sam_compnt_name, act_sam_compnt_place_in_series, act_reach_length, act_reach_length_unit, act_reach_width, act_reach_width_unit,
                           act_pass_count, net_type_name, act_net_surface_area, act_net_surface_area_unit, act_net_mesh_size, act_net_mesh_size_unit, act_boat_speed,
                           act_boat_speed_unit, act_current_speed, act_current_speed_unit, toxicity_test_type_name,
                           sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name,
                           act_sam_collect_meth_qual_type, act_sam_collect_meth_desc, sample_collect_equip_name, act_sam_collect_equip_comments, act_sam_prep_meth_id,
                           act_sam_prep_meth_context, act_sam_prep_meth_name, act_sam_prep_meth_qual_type, act_sam_prep_meth_desc, sample_container_type,
                           sample_container_color, act_sam_chemical_preservative, thermal_preservative_name, act_sam_transport_storage_desc,
                           result_id, res_data_logger_line, result_detection_condition_tx, method_specification_name, sample_fraction_type, result_measure_value,
                           result_unit, result_meas_qual_code, result_value_status, statistic_type, result_value_type, weight_basis_type, duration_basis,
                           temperature_basis_level, particle_size, precision, res_measure_bias, res_measure_conf_interval, res_measure_upper_conf_limit,
                           res_measure_lower_conf_limit, result_comment, result_depth_meas_value, result_depth_meas_unit_code, result_depth_alt_ref_pt_txt,
                           res_sampling_point_name, biological_intent, res_bio_individual_id, sample_tissue_taxonomic_name, unidentifiedspeciesidentifier,
                           sample_tissue_anatomy_name, res_group_summary_ct_wt, res_group_summary_ct_wt_unit, cell_form_name, cell_shape_name, habit_name, volt_name,
                           rtdet_pollution_tolerance, rtdet_pollution_tolernce_scale, rtdet_trophic_level, rtfgrp_functional_feeding_grp, taxon_citation_title,
                           taxon_citation_creator, taxon_citation_subject, taxon_citation_publisher, taxon_citation_date, taxon_citation_id, fcdsc_name,
                           frequency_class_unit, fcdsc_lower_bound, fcdsc_upper_bound, analytical_procedure_id, analytical_procedure_source, analytical_method_name,
                           anlmth_qual_type, analytical_method_citation, lab_name, analysis_start_date, analysis_start_time, analysis_start_timezone, analysis_end_date,
                           analysis_end_time, analysis_end_timezone, rlcom_cd, lab_remark, detection_limit, detection_limit_unit, detection_limit_desc,
                           res_lab_accred_yn, res_lab_accred_authority, res_taxonomist_accred_yn, res_taxonomist_accred_authorty, prep_method_id, prep_method_context,
                           prep_method_name, prep_method_qual_type, prep_method_desc, analysis_prep_date_tx, prep_start_time, prep_start_timezone, prep_end_date,
                           prep_end_time, prep_end_timezone, prep_dilution_factor, project_name, monitoring_location_name,
                           result_object_name, result_object_type, result_file_url,
                           last_updated, res_detect_qnt_lmt_url,
                           lab_sample_prep_url, frequency_class_code_1, frequency_class_code_2, frequency_class_code_3,
                           frequency_class_unit_1, frequency_class_unit_2, frequency_class_unit_3, frequency_class_lower_bound_1, frequency_class_lower_bound_2,
                           frequency_class_lower_bound_3, frequency_class_upper_bound_1, frequency_class_upper_bound_2, frequency_class_upper_bound_3)
select activity_swap_storet.data_source_id,
       activity_swap_storet.data_source,
       activity_swap_storet.station_id,
       activity_swap_storet.site_id,
       activity_swap_storet.event_date,
       analytical_method_plus_nemi.nemi_url analytical_method,
       activity_swap_storet.activity,
       characteristic."CHR_NAME" characteristic_name,
       coalesce(characteristic_group."CHRGRP_NAME", 'Not Assigned') characteristic_type,
       activity_swap_storet.sample_media,
       activity_swap_storet.organization,
       activity_swap_storet.site_type,
       activity_swap_storet.huc,
       activity_swap_storet.governmental_unit_code,
       activity_swap_storet.geom,
       activity_swap_storet.organization_name,
       activity_swap_storet.activity_id,
       activity_swap_storet.activity_type_code,
       activity_swap_storet.activity_media_subdiv_name,
       activity_swap_storet.activity_start_time,
       activity_swap_storet.act_start_time_zone,
       activity_swap_storet.activity_stop_date,
       activity_swap_storet.activity_stop_time,
       activity_swap_storet.act_stop_time_zone,
       activity_swap_storet.activity_relative_depth_name,
       activity_swap_storet.activity_depth,
       activity_swap_storet.activity_depth_unit,
       activity_swap_storet.activity_depth_ref_point,
       activity_swap_storet.activity_upper_depth,
       activity_swap_storet.activity_upper_depth_unit,
       activity_swap_storet.activity_lower_depth,
       activity_swap_storet.activity_lower_depth_unit,
       activity_swap_storet.project_id,
       activity_swap_storet.activity_conducting_org,
       activity_swap_storet.activity_comment,
       activity_swap_storet.activity_latitude,
       activity_swap_storet.activity_longitude,
       activity_swap_storet.activity_source_map_scale,
       activity_swap_storet.act_horizontal_accuracy,
       activity_swap_storet.act_horizontal_accuracy_unit,
       activity_swap_storet.act_horizontal_collect_method,
       activity_swap_storet.act_horizontal_datum_name,
       activity_swap_storet.assemblage_sampled_name,
       activity_swap_storet.act_collection_duration,
       activity_swap_storet.act_collection_duration_unit,
       activity_swap_storet.act_sam_compnt_name,
       activity_swap_storet.act_sam_compnt_place_in_series,
       activity_swap_storet.act_reach_length,
       activity_swap_storet.act_reach_length_unit,
       activity_swap_storet.act_reach_width,
       activity_swap_storet.act_reach_width_unit,
       activity_swap_storet.act_pass_count,
       activity_swap_storet.net_type_name,
       activity_swap_storet.act_net_surface_area,
       activity_swap_storet.act_net_surface_area_unit,
       activity_swap_storet.act_net_mesh_size,
       activity_swap_storet.act_net_mesh_size_unit,
       activity_swap_storet.act_boat_speed,
       activity_swap_storet.act_boat_speed_unit,
       activity_swap_storet.act_current_speed,
       activity_swap_storet.act_current_speed_unit,
       activity_swap_storet.toxicity_test_type_name,
       activity_swap_storet.sample_collect_method_id,
       activity_swap_storet.sample_collect_method_ctx,
       activity_swap_storet.sample_collect_method_name,
       activity_swap_storet.act_sam_collect_meth_qual_type,
       activity_swap_storet.act_sam_collect_meth_desc,
       activity_swap_storet.sample_collect_equip_name,
       activity_swap_storet.act_sam_collect_equip_comments,
       activity_swap_storet.act_sam_prep_meth_id,
       activity_swap_storet.act_sam_prep_meth_context,
       activity_swap_storet.act_sam_prep_meth_name,
       activity_swap_storet.act_sam_prep_meth_qual_type,
       activity_swap_storet.act_sam_prep_meth_desc,
       activity_swap_storet.sample_container_type,
       activity_swap_storet.sample_container_color,
       activity_swap_storet.act_sam_chemical_preservative,
       activity_swap_storet.thermal_preservative_name,
       activity_swap_storet.act_sam_transport_storage_desc,
       result."RES_UID" result_id,
       result."RES_DATA_LOGGER_LINE" res_data_logger_line,
       result_detection_condition."RDCND_NAME" result_detection_condition_tx,
       method_speciation."MTHSPC_NAME" method_specification_name,
       sample_fraction."SMFRC_NAME" sample_fraction_type,
       result."RES_MEASURE" result_measure_value,
       rmeasurement_unit."MSUNT_CD" result_unit,
       (select string_agg("MSRQLF_CD", ';' order by "MSRQLF_CD")
            from wqx_dump."RESULT_MEASURE_QUALIFIER" result_measure_qualifier
        	  left join wqx_dump."MEASURE_QUALIFIER" measure_qualifier
                on result_measure_qualifier."MSRQLF_UID" = measure_qualifier."MSRQLF_UID"
        	  left join activity_storet
        	    on result."ACT_UID" = activity_storet.activity_id
        	  left join wqx_dump."CHARACTERISTIC" characteristic
                on result."CHR_UID" = characteristic."CHR_UID"
        	where result."RES_UID" = result_measure_qualifier."RES_UID") result_meas_qual_code,
       result_status."RESSTA_NAME" result_value_status,
       result_statistical_base."RSBAS_CD" statistic_type,
       result_value_type."RVTYP_NAME" result_value_type,
       result_weight_basis."RWBAS_NAME" weight_basis_type,
       result_time_basis."RTIMB_NAME" duration_basis,
       result_temperature_basis."RTMPB_NAME" temperature_basis_level,
       result."RES_PARTICLE_SIZE_BASIS" particle_size,
       result."RES_MEASURE_PRECISION" "precision",
       result."RES_MEASURE_BIAS",
       result."RES_MEASURE_CONF_INTERVAL",
       result."RES_MEASURE_UPPER_CONF_LIMIT",
       result."RES_MEASURE_LOWER_CONF_LIMIT",
       result."RES_COMMENTS" result_comment,
       result."RES_DEPTH_HEIGHT" result_depth_meas_value,
       dhmeasurement_unit."MSUNT_CD" result_depth_meas_unit_code,
       result."RES_DEPTH_ALTITUDE_REF_POINT" result_depth_alt_ref_pt_txt,
       result."RES_SAMPLING_POINT_NAME" res_sampling_point_name,
       biological_intent."BIOINT_NAME" biological_intent,
       result."RES_BIO_INDIVIDUAL_ID" res_bio_individual_id,
       taxon."TAX_NAME" sample_tissue_taxonomic_name,
       result."RES_SPECIES_ID" UnidentifiedSpeciesIdentifier,
       sample_tissue_anatomy."STANT_NAME" sample_tissue_anatomy_name,
       result."RES_GROUP_SUMMARY_CT_WT" res_group_summary_ct_wt,
       group_summ_ct_wt."MSUNT_CD" res_group_summary_ct_wt_unit,
       cell_form."CELFRM_NAME" cell_form_name,
       cell_shape."CELSHP_NAME" cell_shape_name,
       result_taxon_habit_aggregated.habit_name_list habit_name,
       voltinism."VOLT_NAME",
       result_taxon_detail."RTDET_POLLUTION_TOLERANCE" rtdet_pollution_tolerance,
       result_taxon_detail."RTDET_POLLUTION_TOLERNCE_SCALE" rtdet_pollution_tolernce_scale,
       result_taxon_detail."RTDET_TROPHIC_LEVEL" rtdet_trophic_level,
       result_taxon_feeding_group_aggregated.feeding_group_list rtfgrp_functional_feeding_grp,
       taxon_citation."CITATN_TITLE" citatn_title,
       taxon_citation."CITATN_CREATOR" citatn_creator,
       taxon_citation."CITATN_SUBJECT" citatn_subject,
       taxon_citation."CITATN_PUBLISHER" citatn_publisher,
       taxon_citation."CITATN_DATE" citatn_date,
       taxon_citation."CITATN_ID" citatn_id,
       /* FrequencyClassInformation 0-3 of these per BiologicalResultDescription*/
       /*frequency_class_descriptor.*/ null fcdsc_name,
       /*result_frequency.msunt_cd*/ null frequency_class_unit,
       /*result_frequency_class.*/ null fcdsc_lower_bound,
       /*result_frequency_class.*/ null fcdsc_upper_bound,
       analytical_method_plus_nemi.anlmth_id analytical_procedure_id,
       analytical_method_plus_nemi.amctx_cd analytical_procedure_source,
       analytical_method_plus_nemi.anlmth_name analytical_method_name,
       analytical_method_plus_nemi.anlmth_qual_type,
       analytical_method_plus_nemi.anlmth_url analytical_method_citation,
       result."RES_LAB_NAME" lab_name,
       to_char(result."RES_LAB_ANALYSIS_START_DATE", 'yyyy-mm-dd') analysis_start_date,
       result."RES_LAB_ANALYSIS_START_TIME",
       analysis_start."TMZONE_CD" analysis_start_timezone,
       result."RES_LAB_ANALYSIS_END_DATE" res_lab_analysis_end_date,
       result."RES_LAB_ANALYSIS_END_TIME" res_lab_analysis_end_time,
       analysis_end."TMZONE_CD" analysis_end_timezone,
       result_lab_comment."RLCOM_CD" rlcom_cd,
       result_lab_comment."RLCOM_DESC" lab_remark,
       wqx.detection_quant_limit.rdqlmt_measure detection_limit,
       wqx.detection_quant_limit.msunt_cd detection_limit_unit,
       wqx.detection_quant_limit.dqltyp_name detection_limit_desc,
       result."RES_LAB_ACCRED_YN" res_lab_accred_yn,
       result."RES_LAB_ACCRED_AUTHORITY" res_lab_accred_authority,
       result."RES_TAXONOMIST_ACCRED_YN" res_taxonomist_accred_yn,
       result."RES_TAXONOMIST_ACCRED_AUTHORTY" res_taxonomist_accred_authorty,
       /* LabSamplePreparation 0-many per result */
       /*result_lab_sample_prep.*/ null rlsprp_method_id,
       /*result_lab_sample_prep.*/ null rlsprp_method_context,
       /*result_lab_sample_prep.*/ null rlsprp_method_name,
       /*result_lab_sample_prep.*/ null rlsprp_method_qual_type,
       /*result_lab_sample_prep.*/ null rlsprp_method_desc,
       /*to_char(result_lab_sample_prep.rlsprp_start_date, 'yyyy-mm-dd')*/ null analysis_prep_date_tx,
       /*result_lab_sample_prep.*/ null rlsprp_start_time,
       /*prep_start.tmzone_cd*/ null prep_start_timezone,
       /*result_lab_sample_prep.*/ null rlsprp_end_date,
       /*result_lab_sample_prep.*/ null rlsprp_end_time,
       /*prep_end.tmzone_cd*/ null prep_end_time,
       /*result_lab_sample_prep.*/ null rlsprp_dilution_factor,
       activity_swap_storet.project_name,
       activity_swap_storet.monitoring_location_name,
       attached_object_result.result_object_name,
       attached_object_result.result_object_type,
       case
         when attached_object_result.ref_uid is null
           then null
         else
           '/providers/' || coalesce(encode_uri_component(activity_swap_storet.data_source), '') ||
             '/organizations/' || coalesce(encode_uri_component(activity_swap_storet.organization), '') ||
             '/activities/' || coalesce(encode_uri_component(activity_swap_storet.activity), '') ||
             '/results/' || coalesce(result."RES_UID"::text, '') ||
             '/files'
       end result_file_url,
       result."RES_LAST_CHANGE_DATE" last_updated,
       case
         when detection_quant_limit.res_uid is null
           then null
         else
           '/providers/' || coalesce(encode_uri_component(activity_swap_storet.data_source), '') ||
             '/organizations/' || coalesce(encode_uri_component(activity_swap_storet.organization), '') ||
             '/activities/' || coalesce(encode_uri_component(activity_swap_storet.activity), '') ||
             '/results/' || coalesce(result."RES_UID"::text, '') ||
             '/resdetectqntlmts'
       end res_detect_qnt_lmt_url,
       case
         when result_lab_sample_prep_sum.res_uid is null
           then null
         else
           null
       end lab_sample_prep_url,
       result_frequency_class_aggregated.frequency_class_code_1,
       result_frequency_class_aggregated.frequency_class_code_2,
       result_frequency_class_aggregated.frequency_class_code_3,
       result_frequency_class_aggregated.frequency_class_unit_1,
       result_frequency_class_aggregated.frequency_class_unit_2,
       result_frequency_class_aggregated.frequency_class_unit_3,
       result_frequency_class_aggregated.frequency_class_lower_bound_1,
       result_frequency_class_aggregated.frequency_class_lower_bound_2,
       result_frequency_class_aggregated.frequency_class_lower_bound_3,
       result_frequency_class_aggregated.frequency_class_upper_bound_1,
       result_frequency_class_aggregated.frequency_class_upper_bound_2,
       result_frequency_class_aggregated.frequency_class_upper_bound_3
  from activity_swap_storet
       join wqx_dump."RESULT" result
         on activity_swap_storet.activity_id = result."ACT_UID"
       left join wqx_dump."METHOD_SPECIATION" method_speciation
         on result."MTHSPC_UID" = method_speciation."MTHSPC_UID"
       left join wqx_dump."BIOLOGICAL_INTENT" biological_intent
         on result."BIOINT_UID" = biological_intent."BIOINT_UID"
       left join wqx_dump."CHARACTERISTIC" characteristic
         on result."CHR_UID" = characteristic."CHR_UID"
       left join wqx_dump."RESULT_DETECTION_CONDITION" result_detection_condition
         on result."RDCND_UID" = result_detection_condition."RDCND_UID"
       left join wqx_dump."SAMPLE_FRACTION" sample_fraction
         on result."SMFRC_UID" = sample_fraction."SMFRC_UID"
       left join wqx_dump."MEASUREMENT_UNIT" rmeasurement_unit
         on result."MSUNT_UID_MEASURE" = rmeasurement_unit."MSUNT_UID"
       left join wqx_dump."RESULT_STATUS" result_status
         on result."RESSTA_UID" = result_status."RESSTA_UID"
       left join wqx_dump."RESULT_STATISTICAL_BASE" result_statistical_base
         on result."RSBAS_UID" = result_statistical_base."RSBAS_UID"
       left join wqx_dump."RESULT_VALUE_TYPE" result_value_type
         on result."RVTYP_UID" = result_value_type."RVTYP_UID"
       left join wqx_dump."RESULT_WEIGHT_BASIS" result_weight_basis
         on result."RWBAS_UID" = result_weight_basis."RWBAS_UID"
       left join wqx_dump."RESULT_TIME_BASIS" result_time_basis
         on result."RTIMB_UID" = result_time_basis."RTIMB_UID"
       left join wqx_dump."RESULT_TEMPERATURE_BASIS" result_temperature_basis
         on result."RTMPB_UID" = result_temperature_basis."RTMPB_UID"
       left join wqx_dump."MEASUREMENT_UNIT" dhmeasurement_unit
         on result."MSUNT_UID_DEPTH_HEIGHT" = dhmeasurement_unit."MSUNT_UID"
       left join wqx_dump."MEASUREMENT_UNIT" group_summ_ct_wt
         on result."MSUNT_UID_GROUP_SUMMARY_CT_WT" = group_summ_ct_wt."MSUNT_UID"
       left join wqx.analytical_method_plus_nemi
         on result."ANLMTH_UID" = analytical_method_plus_nemi.anlmth_uid
       left join wqx.detection_quant_limit
         on result."RES_UID" = wqx.detection_quant_limit.res_uid
       left join wqx.result_lab_sample_prep_sum
         on result."RES_UID" = wqx.result_lab_sample_prep_sum.res_uid
       left join wqx_dump."TIME_ZONE" analysis_start
         on result."TMZONE_UID_LAB_ANALYSIS_START" = analysis_start."TMZONE_UID"
       left join wqx_dump."TIME_ZONE" analysis_end
         on result."TMZONE_UID_LAB_ANALYSIS_END" = analysis_end."TMZONE_UID"
       left join wqx_dump."TAXON" taxon
         on result."TAX_UID" = taxon."TAX_UID"
       left join wqx_dump."SAMPLE_TISSUE_ANATOMY" sample_tissue_anatomy
         on result."STANT_UID" = sample_tissue_anatomy."STANT_UID"
       left join wqx_dump."RESULT_LAB_COMMENT" result_lab_comment
         on result."RLCOM_UID" = result_lab_comment."RLCOM_UID"
       left join wqx_dump."CHARACTERISTIC_GROUP" characteristic_group
         on characteristic."CHRGRP_UID" = characteristic_group."CHRGRP_UID"
       left join wqx.result_taxon_habit_aggregated
         on result."RES_UID" = result_taxon_habit_aggregated.res_uid
       left join wqx_dump."RESULT_TAXON_DETAIL" result_taxon_detail
         on result."RES_UID" = result_taxon_detail."RES_UID"
       left join wqx_dump."VOLTINISM" voltinism
         on result_taxon_detail."VOLT_UID" = voltinism."VOLT_UID"
       left join wqx.result_taxon_feeding_group_aggregated
         on result."RES_UID" = result_taxon_feeding_group_aggregated.res_uid
       left join wqx_dump."CITATION" taxon_citation
         on result_taxon_detail."CITATN_UID" = taxon_citation."CITATN_UID"
       left join wqx_dump."CELL_FORM" cell_form
         on result_taxon_detail."CELFRM_UID" = cell_form."CELFRM_UID"
       left join wqx_dump."CELL_SHAPE" cell_shape
         on result_taxon_detail."CELSHP_UID" = cell_shape."CELSHP_UID"
       left join wqx.attached_object_result
         on result."ORG_UID" = attached_object_result.org_uid and
            result."RES_UID" = attached_object_result.ref_uid
       left join wqx.result_frequency_class_aggregated
         on result."RES_UID" = result_frequency_class_aggregated.res_uid
 where result."RESSTA_UID" != 4
