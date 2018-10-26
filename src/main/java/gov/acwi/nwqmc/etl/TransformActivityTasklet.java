package gov.acwi.nwqmc.etl;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

public class TransformActivityTasklet implements Tasklet, InitializingBean {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformActivityTasklet(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

    public RepeatStatus execute(StepContribution contribution,
                                ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
        		"  into activity_swap_biodata (data_source_id, data_source, station_id, site_id, event_date, activity, sample_media,\n" + 
        		"                              organization, site_type, huc, governmental_unit_code, organization_name, activity_id,\n" + 
        		"                              activity_type_code, activity_media_subdiv_name, activity_start_time, act_start_time_zone,\n" + 
        		"                              activity_stop_date, activity_stop_time, act_stop_time_zone, activity_relative_depth_name,\n" + 
        		"                              activity_depth, activity_depth_unit, activity_depth_ref_point, activity_upper_depth,\n" + 
        		"                              activity_upper_depth_unit, activity_lower_depth, activity_lower_depth_unit, project_id,\n" + 
        		"                              activity_conducting_org, activity_comment, sample_aqfr_name, hydrologic_condition_name,\n" + 
        		"                              hydrologic_event_name, activity_latitude, activity_longitude, activity_source_map_scale,\n" + 
        		"                              act_horizontal_accuracy, act_horizontal_accuracy_unit, act_horizontal_collect_method,\n" + 
        		"                              act_horizontal_datum_name, assemblage_sampled_name, act_collection_duration,\n" + 
        		"                              act_collection_duration_unit, act_sam_compnt_name, act_sam_compnt_place_in_series,\n" + 
        		"                              act_reach_length, act_reach_length_unit, act_reach_width, act_reach_width_unit,\n" + 
        		"                              act_pass_count, net_type_name, act_net_surface_area, act_net_surface_area_unit,\n" + 
        		"                              act_net_mesh_size, act_net_mesh_size_unit, act_boat_speed, act_boat_speed_unit,\n" + 
        		"                              act_current_speed, act_current_speed_unit, toxicity_test_type_name,\n" + 
        		"                              sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name,\n" + 
        		"                              act_sam_collect_meth_qual_type, act_sam_collect_meth_desc, sample_collect_equip_name,\n" + 
        		"                              act_sam_collect_equip_comments, act_sam_prep_meth_id, act_sam_prep_meth_context,\n" + 
        		"                              act_sam_prep_meth_name, act_sam_prep_meth_qual_type, act_sam_prep_meth_desc,\n" + 
        		"                              sample_container_type, sample_container_color, act_sam_chemical_preservative,\n" + 
        		"                              thermal_preservative_name, act_sam_transport_storage_desc)\n" + 
        		"select 4 data_source_id,\n" + 
        		"       'BIODATA' data_source,\n" + 
        		"       station.station_id, \n" + 
        		"       station.site_id,\n" + 
        		"       trunc(sample.collection_start) event_date,\n" + 
        		"       sample.sidno || '-' || effort.method_code activity,\n" + 
        		"       'Biological' sample_media,\n" + 
        		"       station.organization,\n" + 
        		"       station.site_type,\n" + 
        		"       station.huc,\n" + 
        		"       station.governmental_unit_code,\n" + 
        		"       station.organization_name,\n" + 
        		"       effort.dw_effort_id activity_id,\n" + 
        		"       'Field Msr/Obs' activity_type_code,\n" + 
        		"       null activity_media_subdiv_name,\n" + 
        		"       case\n" + 
        		"         when sample.data_source = 'BioTDB' then null\n" + 
        		"         else to_char(sample.collection_start, 'hh24:mi:ss')\n" + 
        		"       end  activity_start_time,\n" + 
        		"       case\n" + 
        		"         when sample.data_source = 'BioTDB' then null\n" + 
        		"         else sample.time_datum\n" + 
        		"       end act_start_time_zone,\n" + 
        		"       null activity_stop_date,\n" + 
        		"       null activity_stop_time,\n" + 
        		"       null act_stop_time_zone,\n" + 
        		"       null activity_relative_depth_name,\n" + 
        		"       null activity_depth,\n" + 
        		"       null activity_depth_unit,\n" + 
        		"       null activity_depth_ref_point,\n" + 
        		"       null activity_upper_depth,\n" + 
        		"       null activity_upper_depth_unit,\n" + 
        		"       null activity_lower_depth,\n" + 
        		"       null activity_lower_depth_unit,\n" + 
        		"       project.project_label project_id,\n" + 
        		"       null activity_conducting_org,\n" + 
        		"       effort.comments activity_comment,\n" + 
        		"       null sample_aqfr_name,\n" + 
        		"       null hydrologic_condition_name,\n" + 
        		"       null hydrologic_event_name,\n" + 
        		"       null activity_latitude,\n" + 
        		"       null activity_longitude,\n" + 
        		"       null activity_source_map_scale,\n" + 
        		"       null act_horizontal_accuracy,\n" + 
        		"       null act_horizontal_accuracy_unit,\n" + 
        		"       null act_horizontal_collect_method,\n" + 
        		"       null act_horizontal_datum_name,\n" + 
        		"       'Fish/Nekton' assemblage_sampled_name,\n" + 
        		"       null act_collection_duration,\n" + 
        		"       null act_collection_duration_unit,\n" + 
        		"       null act_sam_compnt_name,\n" + 
        		"       null act_sam_compnt_place_in_series,\n" + 
        		"       sample.reach_length_fished act_reach_length,\n" + 
        		"       case\n" + 
        		"         when sample.reach_length_fished is null then null\n" + 
        		"         else 'm'\n" + 
        		"       end act_reach_length_unit,\n" + 
        		"       null act_reach_width,\n" + 
        		"       null act_reach_width_unit,\n" + 
        		"       case\n" + 
        		"         when effort.pass = 'Pass 1 & 2 combined' then '2'\n" + 
        		"         else '1'\n" + 
        		"       end act_pass_count,\n" + 
        		"       null net_type_name,\n" + 
        		"       null act_net_surface_area,\n" + 
        		"       null act_net_surface_area_unit,\n" + 
        		"       null act_net_mesh_size,\n" + 
        		"       null act_net_mesh_size_unit,\n" + 
        		"       null act_boat_speed,\n" + 
        		"       null act_boat_speed_unit,\n" + 
        		"       null act_current_speed,\n" + 
        		"       null act_current_speed_unit,\n" + 
        		"       null toxicity_test_type_name,\n" + 
        		"       effort.method_code sample_collect_method_id,\n" + 
        		"       sample.sampling_method_reference || ' ' || effort.method_code sample_collect_method_ctx,\n" + 
        		"       sample.sampling_method_reference sample_collect_method_name,\n" + 
        		"       null act_sam_collect_meth_qual_type,\n" + 
        		"       sample.sampling_method_ref_citation act_sam_collect_meth_desc,\n" + 
        		"       case lower(nvl(effort.gear, sample.gear_used))\n" + 
        		"         when 'backpack' then 'Backpack Electroshock'\n" + 
        		"         when 'towed barge' then 'Electroshock (Other)'\n" + 
        		"         when 'boat' then 'Boat-Mounted Electroshock'\n" + 
        		"         when 'minnow seine' then 'Minnow Seine Net'\n" + 
        		"         when 'bag seine' then 'Seine Net'\n" + 
        		"         when 'beach seine' then 'Beach Seine Net'\n" + 
        		"         when 'snorkeling' then 'Visual Sighting'\n" + 
        		"         else null\n" + 
        		"       end sample_collect_equip_name,\n" + 
        		"       nvl(effort.gear, sample.gear_used) || \n" + 
        		"          case \n" + 
        		"            when sample.dw_sample_type_id = 16\n" + 
        		"              then case \n" + 
        		"                     when effort.subreach is not null\n" + 
        		"                       then '+' || effort.subreach\n" + 
        		"                     else null\n" + 
        		"                   end\n" + 
        		"            when effort.pass is not null\n" + 
        		"              then '+' || effort.pass\n" + 
        		"            else null\n" + 
        		"          end act_sam_collect_equip_comments,\n" + 
        		"       null act_sam_prep_meth_id,\n" + 
        		"       null act_sam_prep_meth_context,\n" + 
        		"       null act_sam_prep_meth_name,\n" + 
        		"       null act_sam_prep_meth_qual_type,\n" + 
        		"       null act_sam_prep_meth_desc,\n" + 
        		"       null sample_container_type,\n" + 
        		"       null sample_container_color,\n" + 
        		"       null act_sam_chemical_preservative,\n" + 
        		"       null thermal_preservative_name,\n" + 
        		"       null act_sam_transport_storage_desc\n" + 
        		"  from biodata.effort\n" + 
        		"       join biodata.sample\n" + 
        		"         on effort.dw_sample_id = sample.dw_sample_id\n" + 
        		"       join station_swap_biodata station\n" + 
        		"         on sample.biodata_site_id = station.station_id\n" + 
        		"       join biodata.project\n" + 
        		"         on sample.dw_project_id = project.dw_project_id\n" + 
        		" where sample.data_release_category = 'Public' and\n" + 
        		"       project.data_release_category = 'Public' and\n" + 
        		"       sample.dw_sample_type_id in (7, 15, 16, 24)");
        return RepeatStatus.FINISHED;
    }

	@Override
	public void afterPropertiesSet() throws Exception {
		// TODO Auto-generated method stub
		
	}
}
