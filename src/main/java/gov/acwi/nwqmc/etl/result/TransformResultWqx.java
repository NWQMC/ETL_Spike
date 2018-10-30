package gov.acwi.nwqmc.etl.result;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@StepScope
public class TransformResultWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformResultWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into result_swap_storet (data_source_id, data_source, station_id, site_id, event_date, analytical_method, activity,\n" + 
				"                           characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code,\n" + 
				"                           organization_name, activity_id, activity_type_code, activity_media_subdiv_name, activity_start_time,\n" + 
				"                           act_start_time_zone, activity_stop_date, activity_stop_time, act_stop_time_zone, activity_relative_depth_name, activity_depth,\n" + 
				"                           activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,\n" + 
				"                           activity_lower_depth, activity_lower_depth_unit, project_id, activity_conducting_org, activity_comment,\n" + 
				"                           activity_latitude, activity_longitude, activity_source_map_scale, act_horizontal_accuracy, act_horizontal_accuracy_unit,\n" + 
				"                           act_horizontal_collect_method, act_horizontal_datum_name, assemblage_sampled_name, act_collection_duration, act_collection_duration_unit,\n" + 
				"                           act_sam_compnt_name, act_sam_compnt_place_in_series, act_reach_length, act_reach_length_unit, act_reach_width, act_reach_width_unit,\n" + 
				"                           act_pass_count, net_type_name, act_net_surface_area, act_net_surface_area_unit, act_net_mesh_size, act_net_mesh_size_unit, act_boat_speed,\n" + 
				"                           act_boat_speed_unit, act_current_speed, act_current_speed_unit, toxicity_test_type_name,\n" + 
				"                           sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name,\n" + 
				"                           act_sam_collect_meth_qual_type, act_sam_collect_meth_desc, sample_collect_equip_name, act_sam_collect_equip_comments, act_sam_prep_meth_id,\n" + 
				"                           act_sam_prep_meth_context, act_sam_prep_meth_name, act_sam_prep_meth_qual_type, act_sam_prep_meth_desc, sample_container_type,\n" + 
				"                           sample_container_color, act_sam_chemical_preservative, thermal_preservative_name, act_sam_transport_storage_desc,\n" + 
				"                           result_id, res_data_logger_line, result_detection_condition_tx, method_specification_name, sample_fraction_type, result_measure_value,\n" + 
				"                           result_unit, result_meas_qual_code, result_value_status, statistic_type, result_value_type, weight_basis_type, duration_basis,\n" + 
				"                           temperature_basis_level, particle_size, precision, res_measure_bias, res_measure_conf_interval, res_measure_upper_conf_limit,\n" + 
				"                           res_measure_lower_conf_limit, result_comment, result_depth_meas_value, result_depth_meas_unit_code, result_depth_alt_ref_pt_txt,\n" + 
				"                           res_sampling_point_name, biological_intent, res_bio_individual_id, sample_tissue_taxonomic_name, unidentifiedspeciesidentifier,\n" + 
				"                           sample_tissue_anatomy_name, res_group_summary_ct_wt, res_group_summary_ct_wt_unit, cell_form_name, cell_shape_name, habit_name, volt_name,\n" + 
				"                           rtdet_pollution_tolerance, rtdet_pollution_tolernce_scale, rtdet_trophic_level, rtfgrp_functional_feeding_grp, taxon_citation_title,\n" + 
				"                           taxon_citation_creator, taxon_citation_subject, taxon_citation_publisher, taxon_citation_date, taxon_citation_id, fcdsc_name,\n" + 
				"                           frequency_class_unit, fcdsc_lower_bound, fcdsc_upper_bound, analytical_procedure_id, analytical_procedure_source, analytical_method_name,\n" + 
				"                           anlmth_qual_type, analytical_method_citation, lab_name, analysis_start_date, analysis_start_time, analysis_start_timezone, analysis_end_date,\n" + 
				"                           analysis_end_time, analysis_end_timezone, rlcom_cd, lab_remark, detection_limit, detection_limit_unit, detection_limit_desc,\n" + 
				"                           res_lab_accred_yn, res_lab_accred_authority, res_taxonomist_accred_yn, res_taxonomist_accred_authorty, prep_method_id, prep_method_context,\n" + 
				"                           prep_method_name, prep_method_qual_type, prep_method_desc, analysis_prep_date_tx, prep_start_time, prep_start_timezone, prep_end_date,\n" + 
				"                           prep_end_time, prep_end_timezone, prep_dilution_factor, project_name, monitoring_location_name, result_object_name, result_object_type,\n" + 
				"                           result_file_url, last_updated, res_detect_qnt_lmt_url, lab_sample_prep_url, frequency_class_code_1, frequency_class_code_2, frequency_class_code_3,\n" + 
				"                           frequency_class_unit_1, frequency_class_unit_2, frequency_class_unit_3, frequency_class_lower_bound_1, frequency_class_lower_bound_2,\n" + 
				"                           frequency_class_lower_bound_3, frequency_class_upper_bound_1, frequency_class_upper_bound_2, frequency_class_upper_bound_3)\n" + 
				"select /*+ parallel(4) */\n" + 
				"       activity_swap_storet.data_source_id,\n" + 
				"       activity_swap_storet.data_source,\n" + 
				"       activity_swap_storet.station_id, \n" + 
				"       activity_swap_storet.site_id,\n" + 
				"       activity_swap_storet.event_date,\n" + 
				"       wqx_analytical_method.nemi_url analytical_method,\n" + 
				"       activity_swap_storet.activity,\n" + 
				"       characteristic.chr_name characteristic_name,\n" + 
				"       nvl(di_characteristic.characteristic_group_type, 'Not Assigned') characteristic_type,\n" + 
				"       activity_swap_storet.sample_media,\n" + 
				"       activity_swap_storet.organization,\n" + 
				"       activity_swap_storet.site_type,\n" + 
				"       activity_swap_storet.huc,\n" + 
				"       activity_swap_storet.governmental_unit_code,\n" + 
				"       activity_swap_storet.organization_name,\n" + 
				"       activity_swap_storet.activity_id,\n" + 
				"       activity_swap_storet.activity_type_code,\n" + 
				"       activity_swap_storet.activity_media_subdiv_name,\n" + 
				"       activity_swap_storet.activity_start_time,\n" + 
				"       activity_swap_storet.act_start_time_zone,\n" + 
				"       activity_swap_storet.activity_stop_date,\n" + 
				"       activity_swap_storet.activity_stop_time,\n" + 
				"       activity_swap_storet.act_stop_time_zone,\n" + 
				"       activity_swap_storet.activity_relative_depth_name,\n" + 
				"       activity_swap_storet.activity_depth,\n" + 
				"       activity_swap_storet.activity_depth_unit,\n" + 
				"       activity_swap_storet.activity_depth_ref_point,\n" + 
				"       activity_swap_storet.activity_upper_depth,\n" + 
				"       activity_swap_storet.activity_upper_depth_unit,\n" + 
				"       activity_swap_storet.activity_lower_depth,\n" + 
				"       activity_swap_storet.activity_lower_depth_unit,\n" + 
				"       activity_swap_storet.project_id,\n" + 
				"       activity_swap_storet.activity_conducting_org,\n" + 
				"       activity_swap_storet.activity_comment,\n" + 
				"       activity_swap_storet.activity_latitude,\n" + 
				"       activity_swap_storet.activity_longitude,\n" + 
				"       activity_swap_storet.activity_source_map_scale,\n" + 
				"       activity_swap_storet.act_horizontal_accuracy,\n" + 
				"       activity_swap_storet.act_horizontal_accuracy_unit,\n" + 
				"       activity_swap_storet.act_horizontal_collect_method,\n" + 
				"       activity_swap_storet.act_horizontal_datum_name,\n" + 
				"       activity_swap_storet.assemblage_sampled_name,\n" + 
				"       activity_swap_storet.act_collection_duration,\n" + 
				"       activity_swap_storet.act_collection_duration_unit,\n" + 
				"       activity_swap_storet.act_sam_compnt_name,\n" + 
				"       activity_swap_storet.act_sam_compnt_place_in_series,\n" + 
				"       activity_swap_storet.act_reach_length,\n" + 
				"       activity_swap_storet.act_reach_length_unit,\n" + 
				"       activity_swap_storet.act_reach_width,\n" + 
				"       activity_swap_storet.act_reach_width_unit,\n" + 
				"       activity_swap_storet.act_pass_count,\n" + 
				"       activity_swap_storet.net_type_name,\n" + 
				"       activity_swap_storet.act_net_surface_area,\n" + 
				"       activity_swap_storet.act_net_surface_area_unit,\n" + 
				"       activity_swap_storet.act_net_mesh_size,\n" + 
				"       activity_swap_storet.act_net_mesh_size_unit,\n" + 
				"       activity_swap_storet.act_boat_speed,\n" + 
				"       activity_swap_storet.act_boat_speed_unit,\n" + 
				"       activity_swap_storet.act_current_speed,\n" + 
				"       activity_swap_storet.act_current_speed_unit,\n" + 
				"       activity_swap_storet.toxicity_test_type_name,\n" + 
				"       activity_swap_storet.sample_collect_method_id,\n" + 
				"       activity_swap_storet.sample_collect_method_ctx,\n" + 
				"       activity_swap_storet.sample_collect_method_name,\n" + 
				"       activity_swap_storet.act_sam_collect_meth_qual_type,\n" + 
				"       activity_swap_storet.act_sam_collect_meth_desc,\n" + 
				"       activity_swap_storet.sample_collect_equip_name,\n" + 
				"       activity_swap_storet.act_sam_collect_equip_comments,\n" + 
				"       activity_swap_storet.act_sam_prep_meth_id,\n" + 
				"       activity_swap_storet.act_sam_prep_meth_context,\n" + 
				"       activity_swap_storet.act_sam_prep_meth_name,\n" + 
				"       activity_swap_storet.act_sam_prep_meth_qual_type,\n" + 
				"       activity_swap_storet.act_sam_prep_meth_desc,\n" + 
				"       activity_swap_storet.sample_container_type,\n" + 
				"       activity_swap_storet.sample_container_color,\n" + 
				"       activity_swap_storet.act_sam_chemical_preservative,\n" + 
				"       activity_swap_storet.thermal_preservative_name,\n" + 
				"       activity_swap_storet.act_sam_transport_storage_desc,\n" + 
				"       result.res_uid result_id,\n" + 
				"       result.res_data_logger_line,\n" + 
				"       result_detection_condition.rdcnd_name result_detection_condition_tx,\n" + 
				"       method_speciation.mthspc_name method_specification_name,\n" + 
				"       sample_fraction.smfrc_name sample_fraction_type,\n" + 
				"       result.res_measure result_measure_value,\n" + 
				"       rmeasurement_unit.msunt_cd result_unit,\n" + 
				"       result_measure_qualifier.rmqlf_cd result_meas_qual_code,\n" + 
				"       result_status.ressta_name result_value_status,\n" + 
				"       result_statistical_base.rsbas_cd statistic_type,\n" + 
				"       result_value_type.rvtyp_name result_value_type,\n" + 
				"       result_weight_basis.rwbas_name weight_basis_type,\n" + 
				"       result_time_basis.rtimb_name duration_basis,\n" + 
				"       result_temperature_basis.rtmpb_name temperature_basis_level,\n" + 
				"       result.res_particle_size_basis particle_size,\n" + 
				"       result.res_measure_precision precision,\n" + 
				"       result.res_measure_bias,\n" + 
				"       result.res_measure_conf_interval,\n" + 
				"       result.res_measure_upper_conf_limit,\n" + 
				"       result.res_measure_lower_conf_limit,\n" + 
				"       result.res_comments result_comment,\n" + 
				"       result.res_depth_height result_depth_meas_value,\n" + 
				"       dhmeasurement_unit.msunt_cd result_depth_meas_unit_code,\n" + 
				"       result.res_depth_altitude_ref_point result_depth_alt_ref_pt_txt,\n" + 
				"       result.res_sampling_point_name,\n" + 
				"       biological_intent.bioint_name biological_intent,\n" + 
				"       result.res_bio_individual_id,\n" + 
				"       taxon.tax_name sample_tissue_taxonomic_name,\n" + 
				"       result.res_species_id UnidentifiedSpeciesIdentifier,\n" + 
				"       sample_tissue_anatomy.stant_name sample_tissue_anatomy_name,\n" + 
				"       result.res_group_summary_ct_wt,\n" + 
				"       group_summ_ct_wt.msunt_cd res_group_summary_ct_wt_unit,\n" + 
				"       cell_form.celfrm_name cell_form_name,\n" + 
				"       cell_shape.celshp_name cell_shape_name,\n" + 
				"       wqx_result_taxon_habit.habit_name_list habit_name,\n" + 
				"       voltinism.volt_name,\n" + 
				"       result_taxon_detail.rtdet_pollution_tolerance,\n" + 
				"       result_taxon_detail.rtdet_pollution_tolernce_scale,\n" + 
				"       result_taxon_detail.rtdet_trophic_level,\n" + 
				"       wqx_result_taxon_feeding_group.feeding_group_list rtfgrp_functional_feeding_grp,\n" + 
				"       taxon_citation.citatn_title,\n" + 
				"       taxon_citation.citatn_creator,\n" + 
				"       taxon_citation.citatn_subject,\n" + 
				"       taxon_citation.citatn_publisher,\n" + 
				"       taxon_citation.citatn_date,\n" + 
				"       taxon_citation.citatn_id,\n" + 
				"       /* FrequencyClassInformation 0-3 of these per BiologicalResultDescription*/\n" + 
				"       /*frequency_class_descriptor.*/ null fcdsc_name,\n" + 
				"       /*result_frequency.msunt_cd*/ null frequency_class_unit,\n" + 
				"       /*result_frequency_class.*/ null fcdsc_lower_bound,\n" + 
				"       /*result_frequency_class.*/ null fcdsc_upper_bound,\n" + 
				"       wqx_analytical_method.anlmth_id analytical_procedure_id,\n" + 
				"       wqx_analytical_method.amctx_cd analytical_procedure_source,\n" + 
				"       wqx_analytical_method.anlmth_name analytical_method_name,\n" + 
				"       wqx_analytical_method.anlmth_qual_type,\n" + 
				"       wqx_analytical_method.anlmth_url analytical_method_citation,\n" + 
				"       result.res_lab_name lab_name,\n" + 
				"       to_char(result.res_lab_analysis_start_date, 'yyyy-mm-dd') analysis_start_date,\n" + 
				"       result.res_lab_analysis_start_time,\n" + 
				"       analysis_start.tmzone_cd analysis_start_timezone,\n" + 
				"       result.res_lab_analysis_end_date,\n" + 
				"       result.res_lab_analysis_end_time,\n" + 
				"       analysis_end.tmzone_cd analysis_end_timezone,\n" + 
				"       result_lab_comment.rlcom_cd,\n" + 
				"       result_lab_comment.rlcom_desc lab_remark,\n" + 
				"       wqx_detection_quant_limit.rdqlmt_measure detection_limit,\n" + 
				"       wqx_detection_quant_limit.msunt_cd detection_limit_unit,\n" + 
				"       wqx_detection_quant_limit.dqltyp_name detection_limit_desc,\n" + 
				"       result.res_lab_accred_yn,\n" + 
				"       result.res_lab_accred_authority,\n" + 
				"       result.res_taxonomist_accred_yn,\n" + 
				"       result.res_taxonomist_accred_authorty,\n" + 
				"       /* LabSamplePreparation 0-many per result */\n" + 
				"       /*result_lab_sample_prep.*/ null rlsprp_method_id,\n" + 
				"       /*result_lab_sample_prep.*/ null rlsprp_method_context,\n" + 
				"       /*result_lab_sample_prep.*/ null rlsprp_method_name,\n" + 
				"       /*result_lab_sample_prep.*/ null rlsprp_method_qual_type,\n" + 
				"       /*result_lab_sample_prep.*/ null rlsprp_method_desc,\n" + 
				"       /*to_char(result_lab_sample_prep.rlsprp_start_date, 'yyyy-mm-dd')*/ null analysis_prep_date_tx,\n" + 
				"       /*result_lab_sample_prep.*/ null rlsprp_start_time,\n" + 
				"       /*prep_start.tmzone_cd*/ null prep_start_timezone,\n" + 
				"       /*result_lab_sample_prep.*/ null rlsprp_end_date,\n" + 
				"       /*result_lab_sample_prep.*/ null rlsprp_end_time,\n" + 
				"       /*prep_end.tmzone_cd*/ null prep_end_time,\n" + 
				"       /*result_lab_sample_prep.*/ null rlsprp_dilution_factor,\n" + 
				"       activity_swap_storet.project_name,\n" + 
				"       activity_swap_storet.monitoring_location_name,\n" + 
				"       wqx_attached_object_result.result_object_name,\n" + 
				"       wqx_attached_object_result.result_object_type,\n" + 
				"       case\n" + 
				"         when wqx_attached_object_result.ref_uid is null\n" + 
				"           then null\n" + 
				"         else\n" + 
				"           '/organizations/' ||\n" + 
				"               pkg_dynamic_list.url_escape(activity_swap_storet.organization, 'true') || '/activities/' ||\n" + 
				"               pkg_dynamic_list.url_escape(activity_swap_storet.activity, 'true') || '/results/' ||\n" + 
				"               pkg_dynamic_list.url_escape(activity_swap_storet.organization, 'true') || '-' ||\n" + 
				"               pkg_dynamic_list.url_escape(result.res_uid, 'true') || '/files'\n" + 
				"       end result_file_url,\n" + 
				"       result.res_last_change_date last_updated,\n" + 
				"       case \n" + 
				"         when wqx_detection_quant_limit.res_uid is null\n" + 
				"           then null\n" + 
				"         else \n" + 
				"           '/activities/' ||\n" + 
				"               pkg_dynamic_list.url_escape(activity_swap_storet.activity, 'true') || '/results/' ||\n" + 
				"               pkg_dynamic_list.url_escape(activity_swap_storet.organization, 'true') || '-' ||\n" + 
				"               pkg_dynamic_list.url_escape(result.res_uid, 'true') || '/resdetectqntlmts'\n" + 
				"       end res_detect_qnt_lmt_url,\n" + 
				"       case \n" + 
				"         when wqx_result_lab_sample_prep_sum.res_uid is null\n" + 
				"           then null\n" + 
				"         else\n" + 
				"           null\n" + 
				"       end lab_sample_prep_url,\n" + 
				"       wqx_result_frequency_class.one_fcdsc_name frequency_class_code_1,\n" + 
				"       wqx_result_frequency_class.two_fcdsc_name frequency_class_code_2,\n" + 
				"       wqx_result_frequency_class.three_fcdsc_name frequency_class_code_3,\n" + 
				"       wqx_result_frequency_class.one_msunt_cd frequency_class_unit_1,\n" + 
				"       wqx_result_frequency_class.two_msunt_cd frequency_class_unit_2,\n" + 
				"       wqx_result_frequency_class.three_msunt_cd frequency_class_unit_3,\n" + 
				"       wqx_result_frequency_class.one_fcdsc_lower_bound frequency_class_lower_bound_1,\n" + 
				"       wqx_result_frequency_class.two_fcdsc_lower_bound frequency_class_lower_bound_2,\n" + 
				"       wqx_result_frequency_class.three_fcdsc_lower_bound frequency_class_lower_bound_3,\n" + 
				"       wqx_result_frequency_class.one_fcdsc_upper_bound frequency_class_upper_bound_1,\n" + 
				"       wqx_result_frequency_class.two_fcdsc_upper_bound frequency_class_upper_bound_2,\n" + 
				"       wqx_result_frequency_class.three_fcdsc_upper_bound frequency_class_upper_bound_3\n" + 
				"  from activity_swap_storet\n" + 
				"       join wqx.result\n" + 
				"         on activity_swap_storet.activity_id = result.act_uid\n" + 
				"       left join wqx.method_speciation\n" + 
				"         on result.mthspc_uid = method_speciation.mthspc_uid\n" + 
				"       left join wqx.biological_intent\n" + 
				"         on result.bioint_uid = biological_intent.bioint_uid\n" + 
				"       left join wqx.characteristic\n" + 
				"         on result.chr_uid = characteristic.chr_uid\n" + 
				"       left join wqx.result_detection_condition\n" + 
				"         on result.rdcnd_uid = result_detection_condition.rdcnd_uid\n" + 
				"       left join wqx.sample_fraction\n" + 
				"         on result.smfrc_uid = sample_fraction.smfrc_uid\n" + 
				"       left join wqx.measurement_unit rmeasurement_unit\n" + 
				"         on result.msunt_uid_measure = rmeasurement_unit.msunt_uid\n" + 
				"       left join wqx.result_measure_qualifier\n" + 
				"         on result.rmqlf_uid = result_measure_qualifier.rmqlf_uid\n" + 
				"       left join wqx.result_status\n" + 
				"         on result.ressta_uid = result_status.ressta_uid\n" + 
				"       left join wqx.result_statistical_base\n" + 
				"         on result.rsbas_uid = result_statistical_base.rsbas_uid\n" + 
				"       left join wqx.result_value_type\n" + 
				"         on result.rvtyp_uid = result_value_type.rvtyp_uid\n" + 
				"       left join wqx.result_weight_basis\n" + 
				"         on result.rwbas_uid = result_weight_basis.rwbas_uid\n" + 
				"       left join wqx.result_time_basis\n" + 
				"         on result.rtimb_uid = result_time_basis.rtimb_uid\n" + 
				"       left join wqx.result_temperature_basis\n" + 
				"         on result.rtmpb_uid = result_temperature_basis.rtmpb_uid\n" + 
				"       left join wqx.measurement_unit dhmeasurement_unit\n" + 
				"         on result.msunt_uid_depth_height = dhmeasurement_unit.msunt_uid\n" + 
				"       left join wqx.measurement_unit group_summ_ct_wt\n" + 
				"         on result.msunt_uid_group_summary_ct_wt = group_summ_ct_wt.msunt_uid\n" + 
				"       left join wqx_analytical_method\n" + 
				"         on result.anlmth_uid = wqx_analytical_method.anlmth_uid\n" + 
				"       left join wqx_detection_quant_limit\n" + 
				"         on result.res_uid = wqx_detection_quant_limit.res_uid\n" + 
				"       left join wqx_result_lab_sample_prep_sum\n" + 
				"         on result.res_uid = wqx_result_lab_sample_prep_sum.res_uid\n" + 
				"       left join wqx.time_zone analysis_start\n" + 
				"         on result.tmzone_uid_lab_analysis_start = analysis_start.tmzone_uid\n" + 
				"       left join wqx.time_zone analysis_end\n" + 
				"         on result.tmzone_uid_lab_analysis_end = analysis_end.tmzone_uid \n" + 
				"       left join wqx.taxon\n" + 
				"         on result.tax_uid = taxon.tax_uid\n" + 
				"       left join wqx.sample_tissue_anatomy\n" + 
				"         on result.stant_uid = sample_tissue_anatomy.stant_uid\n" + 
				"       left join wqx.result_lab_comment\n" + 
				"         on result.rlcom_uid = result_lab_comment.rlcom_uid\n" + 
				"       left join storetw.di_characteristic\n" + 
				"         on characteristic.chr_storet_id = di_characteristic.pk_isn\n" + 
				"       left join wqx_result_taxon_habit\n" + 
				"         on result.res_uid = wqx_result_taxon_habit.res_uid\n" + 
				"       left join wqx.result_taxon_detail\n" + 
				"         on result.res_uid = result_taxon_detail.res_uid\n" + 
				"       left join wqx.voltinism\n" + 
				"         on result_taxon_detail.volt_uid = voltinism.volt_uid\n" + 
				"       left join wqx_result_taxon_feeding_group\n" + 
				"         on result.res_uid = wqx_result_taxon_feeding_group.res_uid\n" + 
				"       left join wqx.citation taxon_citation\n" + 
				"         on result_taxon_detail.citatn_uid = taxon_citation.citatn_uid\n" + 
				"       left join wqx.cell_form\n" + 
				"         on result_taxon_detail.celfrm_uid = cell_form.celfrm_uid\n" + 
				"       left join wqx.cell_shape\n" + 
				"         on result_taxon_detail.celshp_uid = cell_shape.celshp_uid\n" + 
				"       left join wqx_attached_object_result\n" + 
				"         on result.org_uid = wqx_attached_object_result.org_uid and\n" + 
				"            result.res_uid = wqx_attached_object_result.ref_uid\n" + 
				"       left join wqx_result_frequency_class\n" + 
				"         on result.res_uid = wqx_result_frequency_class.res_uid\n" + 
				" where result.ressta_uid != 4");
		return RepeatStatus.FINISHED;
	}
}
