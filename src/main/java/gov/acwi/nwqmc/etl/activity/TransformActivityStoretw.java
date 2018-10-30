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
public class TransformActivityStoretw implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformActivityStoretw(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into activity_swap_storet (data_source_id, data_source, station_id, site_id, event_date, activity, sample_media, organization, site_type, huc, governmental_unit_code,\n" + 
				"                             organization_name, activity_id, activity_type_code, activity_media_subdiv_name, activity_start_time, act_start_time_zone, activity_stop_date,\n" + 
				"                             activity_stop_time, act_stop_time_zone, activity_depth, activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,\n" + 
				"                             activity_lower_depth, activity_lower_depth_unit, project_id, activity_conducting_org, activity_comment, sample_aqfr_name, hydrologic_condition_name,\n" + 
				"                             hydrologic_event_name, sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name, sample_collect_equip_name)\n" + 
				"select 3 data_source_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       a.*\n" + 
				"  from (select /*+ parallel(4) */ \n" + 
				"               station.station_id, \n" + 
				"               station.site_id,\n" + 
				"               activity_no_source.event_date,\n" + 
				"               activity_no_source.activity,\n" + 
				"               activity_no_source.sample_media,\n" + 
				"               station.organization,\n" + 
				"               station.site_type,\n" + 
				"               station.huc,\n" + 
				"               station.governmental_unit_code,\n" + 
				"               station.organization_name,\n" + 
				"               activity_no_source.activity_id,\n" + 
				"               activity_no_source.activity_type_code,\n" + 
				"               activity_no_source.activity_media_subdiv_name,\n" + 
				"               activity_no_source.activity_start_time,\n" + 
				"               activity_no_source.act_start_time_zone,\n" + 
				"               activity_no_source.activity_stop_date,\n" + 
				"               activity_no_source.activity_stop_time,\n" + 
				"               activity_no_source.act_stop_time_zone,\n" + 
				"               activity_no_source.activity_depth,\n" + 
				"               activity_no_source.activity_depth_unit,\n" + 
				"               activity_no_source.activity_depth_ref_point,\n" + 
				"               activity_no_source.activity_upper_depth,\n" + 
				"               activity_no_source.activity_upper_depth_unit,\n" + 
				"               activity_no_source.activity_lower_depth,\n" + 
				"               activity_no_source.activity_lower_depth_unit,\n" + 
				"               activity_no_source.project_id,\n" + 
				"               activity_no_source.activity_conducting_org,\n" + 
				"               activity_no_source.activity_comment,\n" + 
				"               activity_no_source.sample_aqfr_name,\n" + 
				"               activity_no_source.hydrologic_condition_name,\n" + 
				"               activity_no_source.hydrologic_event_name,\n" + 
				"               activity_no_source.sample_collect_method_id,\n" + 
				"               activity_no_source.sample_collect_method_ctx,\n" + 
				"               activity_no_source.sample_collect_method_name,\n" + 
				"               activity_no_source.sample_collect_equip_name\n" + 
				"          from activity_no_source\n" + 
				"               join station_swap_storet station\n" + 
				"                 on activity_no_source.station_id + 10000000 = station.station_id) a");
		return RepeatStatus.FINISHED;
	}
}
