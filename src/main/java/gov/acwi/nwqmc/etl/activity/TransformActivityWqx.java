package gov.acwi.nwqmc.etl.activity;

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
public class TransformActivityWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformActivityWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into activity_swap_storet (data_source_id, data_source, station_id, site_id, monitoring_location_name, event_date, activity, sample_media, organization, site_type, huc, governmental_unit_code,\n" + 
				"                             organization_name, activity_id, activity_type_code, activity_media_subdiv_name, activity_start_time, act_start_time_zone,\n" + 
				"                             activity_stop_date, activity_stop_time, act_stop_time_zone, activity_relative_depth_name, activity_depth,\n" + 
				"                             activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit, activity_lower_depth,\n" + 
				"                             activity_lower_depth_unit, project_id, project_name, activity_conducting_org, activity_comment, activity_latitude, activity_longitude,\n" + 
				"                             activity_source_map_scale, act_horizontal_accuracy, act_horizontal_accuracy_unit, act_horizontal_collect_method,\n" + 
				"                             act_horizontal_datum_name, assemblage_sampled_name, act_collection_duration, act_collection_duration_unit,\n" + 
				"                             act_sam_compnt_name, act_sam_compnt_place_in_series, act_reach_length, act_reach_length_unit, act_reach_width,\n" + 
				"                             act_reach_width_unit, act_pass_count, net_type_name, act_net_surface_area, act_net_surface_area_unit, act_net_mesh_size,\n" + 
				"                             act_net_mesh_size_unit, act_boat_speed, act_boat_speed_unit, act_current_speed, act_current_speed_unit,\n" + 
				"                             toxicity_test_type_name, sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name,\n" + 
				"                             act_sam_collect_meth_qual_type, act_sam_collect_meth_desc, sample_collect_equip_name, act_sam_collect_equip_comments,\n" + 
				"                             act_sam_prep_meth_id, act_sam_prep_meth_context, act_sam_prep_meth_name, act_sam_prep_meth_qual_type,\n" + 
				"                             act_sam_prep_meth_desc, sample_container_type, sample_container_color, act_sam_chemical_preservative,\n" + 
				"                             thermal_preservative_name, act_sam_transport_storage_desc, activity_object_name, activity_object_type,\n" + 
				"                             activity_file_url, activity_metric_url)\n" + 
				"select /*+ parallel(4) */ \n" + 
				"       3 data_source_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       activity.mloc_uid station_id, \n" + 
				"       station.site_id,\n" + 
				"       station.station_name,\n" + 
				"       trunc(activity.act_start_date) event_date,\n" + 
				"       station.organization || '-' || activity.act_id activity,\n" + 
				"       activity_media.acmed_name sample_media,\n" + 
				"       station.organization,\n" + 
				"       station.site_type,\n" + 
				"       station.huc,\n" + 
				"       station.governmental_unit_code,\n" + 
				"       organization.org_name organization_name,\n" + 
				"       activity.act_uid activity_id,\n" + 
				"       activity_type.actyp_cd activity_type_code,\n" + 
				"       activity_media_subdivision.amsub_name activity_media_subdiv_name,\n" + 
				"       to_char(activity.act_start_time, 'hh24:mi:ss') activity_start_time,\n" + 
				"       start_time_zone.tmzone_cd act_start_time_zone,\n" + 
				"       to_char(activity.act_end_date, 'yyyy-mm-dd') activity_stop_date,\n" + 
				"       nvl2(act_end_date, to_char(activity.act_end_time, 'hh24:mi:ss'), null) activity_stop_time,\n" + 
				"       end_time_zone.tmzone_cd act_stop_time_zone,\n" + 
				"       relative_depth.reldpth_name activity_relative_depth_name,\n" + 
				"       activity.act_depth_height activity_depth,\n" + 
				"       h_measurement_unit.msunt_cd activity_depth_unit,\n" + 
				"       activity.act_depth_altitude_ref_point activity_depth_ref_point,\n" + 
				"       activity.act_depth_height_top activity_upper_depth,\n" + 
				"       t_measurement_unit.msunt_cd activity_upper_depth_unit,\n" + 
				"       activity.act_depth_height_bottom activity_lower_depth,\n" + 
				"       b_measurement_unit.msunt_cd activity_lower_depth_unit,\n" + 
				"       wqx_activity_project.project_id_list project_id,\n" + 
				"       wqx_activity_project.project_name_list project_name,\n" + 
				"       wqx_activity_conducting_org.acorg_name_list activity_conducting_org,\n" + 
				"       activity.act_comments activity_comment,\n" + 
				"       nvl(activity.act_loc_latitude, station.latitude) activity_latitude,\n" + 
				"       nvl(activity.act_loc_longitude, station.longitude) activity_longitude,\n" + 
				"       activity.act_loc_source_map_scale activity_source_map_scale,\n" + 
				"       activity.act_horizontal_accuracy,\n" + 
				"       activity_horizontal_unit.msunt_cd act_horizontal_accuracy_unit,\n" + 
				"       horizontal_collection_method.hcmth_name act_horizontal_collect_method,\n" + 
				"       horizontal_reference_datum.hrdat_name act_horizontal_datum_name,\n" + 
				"       assemblage.asmblg_name assemblage_sampled_name,\n" + 
				"       activity.act_collection_duration,\n" + 
				"       collection_duration.msunt_cd act_collection_duration_unit,\n" + 
				"       activity.act_sam_compnt_name,\n" + 
				"       activity.act_sam_compnt_place_in_series,\n" + 
				"       activity.act_reach_length,\n" + 
				"       reach_length.msunt_cd act_reach_length_unit,\n" + 
				"       activity.act_reach_width,\n" + 
				"       reach_width.msunt_cd act_reach_width_unit,\n" + 
				"       activity.act_pass_count,\n" + 
				"       net_type.nettyp_name net_type_name,\n" + 
				"       activity.act_net_surface_area,\n" + 
				"       net_surface_unit.msunt_cd act_net_surface_area_unit,\n" + 
				"       activity.act_net_mesh_size,\n" + 
				"       net_mesh.msunt_cd act_net_mesh_size_unit,\n" + 
				"       activity.act_boat_speed,\n" + 
				"       boat_speed.msunt_cd act_boat_speed_unit,\n" + 
				"       activity.act_current_speed,\n" + 
				"       current_speed.msunt_cd act_current_speed_unit,\n" + 
				"       toxicity_test_type.tttyp_name toxicity_test_type_name,\n" + 
				"       case\n" + 
				"         when activity.act_sam_collect_meth_id is not null and\n" + 
				"              activity.act_sam_collect_meth_context is not null\n" + 
				"           then activity.act_sam_collect_meth_id\n" + 
				"         else null\n" + 
				"       end sample_collect_method_id,\n" + 
				"       case\n" + 
				"         when activity.act_sam_collect_meth_id is not null and\n" + 
				"              activity.act_sam_collect_meth_context is not null\n" + 
				"           then activity.act_sam_collect_meth_context\n" + 
				"         else null\n" + 
				"       end sample_collect_method_ctx,\n" + 
				"       case\n" + 
				"         when activity.act_sam_collect_meth_id is not null and\n" + 
				"              activity.act_sam_collect_meth_context is not null\n" + 
				"           then activity.act_sam_collect_meth_name\n" + 
				"         else null\n" + 
				"       end sample_collect_method_name,\n" + 
				"       activity.act_sam_collect_meth_qual_type,\n" + 
				"       activity.act_sam_collect_meth_desc,\n" + 
				"       sample_collection_equip.sceqp_name sample_collect_equip_name,\n" + 
				"       activity.act_sam_collect_equip_comments,\n" + 
				"       activity.act_sam_prep_meth_id,\n" + 
				"       activity.act_sam_prep_meth_context,\n" + 
				"       activity.act_sam_prep_meth_name,\n" + 
				"       activity.act_sam_prep_meth_qual_type,\n" + 
				"       activity.act_sam_prep_meth_desc,\n" + 
				"       container_type.contyp_name sample_container_type,\n" + 
				"       container_color.concol_name sample_container_color,\n" + 
				"       activity.act_sam_chemical_preservative,\n" + 
				"       thermal_preservative.thprsv_name thermal_preservative_name,\n" + 
				"       activity.act_sam_transport_storage_desc,\n" + 
				"       wqx_attached_object_activity.activity_object_name,\n" + 
				"       wqx_attached_object_activity.activity_object_type,\n" + 
				"       case\n" + 
				"         when wqx_attached_object_activity.ref_uid is null\n" + 
				"           then null\n" + 
				"         else\n" + 
				"           '/organizations/' ||\n" + 
				"               pkg_dynamic_list.url_escape(station.organization, 'true') || '/activities/' ||\n" + 
				"               pkg_dynamic_list.url_escape(station.organization, 'true') || '-' ||\n" + 
				"               pkg_dynamic_list.url_escape(activity.act_id, 'true') || '/files'\n" + 
				"       end activity_file_url,\n" + 
				"       case\n" + 
				"         when wqx_activity_metric_sum.act_uid is null\n" + 
				"           then null\n" + 
				"           else\n" + 
				"             '/activities/' ||\n" + 
				"               pkg_dynamic_list.url_escape(station.organization, 'true') || '-' ||\n" + 
				"               pkg_dynamic_list.url_escape(activity.act_id, 'true') || '/activitymetrics'\n" + 
				"       end activity_metric_url\n" + 
				"  from wqx.activity\n" + 
				"       join station_swap_storet station\n" + 
				"         on activity.mloc_uid = station.station_id\n" + 
				"       left join wqx.sample_collection_equip\n" + 
				"         on activity.sceqp_uid = sample_collection_equip.sceqp_uid\n" + 
				"       left join wqx_activity_conducting_org\n" + 
				"         on activity.act_uid = wqx_activity_conducting_org.act_uid\n" + 
				"       left join wqx_activity_project\n" + 
				"         on activity.act_uid = wqx_activity_project.act_uid\n" + 
				"       left join wqx.measurement_unit b_measurement_unit\n" + 
				"         on activity.msunt_uid_depth_height_bottom = b_measurement_unit.msunt_uid\n" + 
				"       left join wqx.measurement_unit t_measurement_unit\n" + 
				"         on activity.msunt_uid_depth_height_top = t_measurement_unit.msunt_uid\n" + 
				"       left join wqx.measurement_unit h_measurement_unit\n" + 
				"         on activity.msunt_uid_depth_height = h_measurement_unit.msunt_uid\n" + 
				"       left join wqx.measurement_unit net_surface_unit\n" + 
				"         on activity.msunt_uid_net_surface_area = net_surface_unit.msunt_uid\n" + 
				"       left join wqx.time_zone end_time_zone\n" + 
				"         on activity.tmzone_uid_end_time = end_time_zone.tmzone_uid\n" + 
				"       left join wqx.time_zone start_time_zone\n" + 
				"         on activity.tmzone_uid_start_time = start_time_zone.tmzone_uid\n" + 
				"       left join wqx.activity_media_subdivision\n" + 
				"         on activity.amsub_uid = activity_media_subdivision.amsub_uid\n" + 
				"       left join wqx.activity_type\n" + 
				"         on activity.actyp_uid = activity_type.actyp_uid\n" + 
				"       left join wqx.organization\n" + 
				"         on activity.org_uid = organization.org_uid\n" + 
				"       left join wqx.activity_media\n" + 
				"         on activity.acmed_uid = activity_media.acmed_uid\n" + 
				"       left join wqx.measurement_unit activity_horizontal_unit\n" + 
				"         on activity.msunt_uid_horizontal_accuracy = activity_horizontal_unit.msunt_uid\n" + 
				"       left join wqx.horizontal_collection_method\n" + 
				"         on activity.hcmth_uid = horizontal_collection_method.hcmth_uid\n" + 
				"       left join wqx.horizontal_reference_datum\n" + 
				"         on activity.hrdat_uid = horizontal_reference_datum.hrdat_uid\n" + 
				"       left join wqx.assemblage\n" + 
				"         on activity.asmblg_uid = assemblage.asmblg_uid \n" + 
				"       left join wqx.measurement_unit collection_duration	 \n" + 
				"         on activity.msunt_uid_collection_duration = collection_duration.msunt_uid\n" + 
				"       left join wqx.measurement_unit reach_length\n" + 
				"         on activity.msunt_uid_reach_length = reach_length.msunt_uid\n" + 
				"       left join wqx.measurement_unit reach_width\n" + 
				"         on activity.msunt_uid_reach_width = reach_width.msunt_uid\n" + 
				"       left join wqx.net_type\n" + 
				"         on activity.nettyp_uid = net_type.nettyp_uid\n" + 
				"       left join wqx.measurement_unit net_mesh\n" + 
				"         on activity.msunt_uid_net_mesh_size = net_mesh.msunt_uid\n" + 
				"       left join wqx.measurement_unit boat_speed\n" + 
				"         on activity.msunt_uid_boat_speed = boat_speed.msunt_uid\n" + 
				"       left join wqx.measurement_unit current_speed\n" + 
				"         on activity.msunt_uid_current_speed = current_speed.msunt_uid\n" + 
				"       left join wqx.toxicity_test_type\n" + 
				"         on activity.tttyp_uid = toxicity_test_type.tttyp_uid\n" + 
				"       left join wqx.container_type\n" + 
				"         on activity.contyp_uid = container_type.contyp_uid\n" + 
				"       left join wqx.container_color\n" + 
				"         on activity.concol_uid = container_color.concol_uid\n" + 
				"       left join wqx.thermal_preservative\n" + 
				"         on activity.thprsv_uid = thermal_preservative.thprsv_uid\n" + 
				"       left join wqx.relative_depth\n" + 
				"         on activity.reldpth_uid = relative_depth.reldpth_uid\n" + 
				"       left join wqx_attached_object_activity\n" + 
				"         on activity.org_uid = wqx_attached_object_activity.org_uid and\n" + 
				"            activity.act_uid = wqx_attached_object_activity.ref_uid\n" + 
				"       left join wqx_activity_metric_sum\n" + 
				"         on activity.act_uid = wqx_activity_metric_sum.act_uid");
		return RepeatStatus.FINISHED;
	}
}
