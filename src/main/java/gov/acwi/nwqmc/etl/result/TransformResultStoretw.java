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
public class TransformResultStoretw implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformResultStoretw(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into result_swap_storet (data_source_id, data_source, station_id, site_id, event_date, analytical_method, activity,\n" + 
				"                           characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code,\n" + 
				"                           organization_name, activity_id, activity_type_code, activity_media_subdiv_name, activity_start_time,\n" + 
				"                           act_start_time_zone, activity_stop_date, activity_stop_time, act_stop_time_zone, activity_depth,\n" + 
				"                           activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,\n" + 
				"                           activity_lower_depth, activity_lower_depth_unit, project_id, activity_conducting_org, activity_comment,\n" + 
				"                           sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name, sample_collect_equip_name,\n" + 
				"                           result_id, result_detection_condition_tx, sample_fraction_type, result_measure_value, result_unit,\n" + 
				"                           result_meas_qual_code, result_value_status, statistic_type, result_value_type, weight_basis_type, duration_basis,\n" + 
				"                           temperature_basis_level, particle_size, precision, result_comment, result_depth_meas_value,\n" + 
				"                           result_depth_meas_unit_code, result_depth_alt_ref_pt_txt, sample_tissue_taxonomic_name,\n" + 
				"                           analytical_procedure_id, analytical_procedure_source, analytical_method_name, lab_name,\n" + 
				"                           analysis_start_date, lab_remark, detection_limit, detection_limit_unit, detection_limit_desc, analysis_prep_date_tx)\n" + 
				"select 3 data_source_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       a.*\n" + 
				"  from (select /*+ parallel(4) */\n" + 
				"               station.station_id,\n" + 
				"               station.site_id,\n" + 
				"               result_no_source.event_date,\n" + 
				"               result_no_source.analytical_method,\n" + 
				"               result_no_source.activity,\n" + 
				"               result_no_source.characteristic_name,\n" + 
				"               result_no_source.characteristic_type,\n" + 
				"               result_no_source.sample_media,\n" + 
				"               station.organization,\n" + 
				"               station.site_type,\n" + 
				"               station.huc,\n" + 
				"               station.governmental_unit_code,\n" + 
				"               station.organization_name,\n" + 
				"               result_no_source.activity_id,\n" + 
				"               result_no_source.activity_type_code,\n" + 
				"               result_no_source.activity_media_subdiv_name,\n" + 
				"               result_no_source.activity_start_time,\n" + 
				"               result_no_source.act_start_time_zone,\n" + 
				"               result_no_source.activity_stop_date,\n" + 
				"               result_no_source.activity_stop_time,\n" + 
				"               result_no_source.act_stop_time_zone,\n" + 
				"               result_no_source.activity_depth,\n" + 
				"               result_no_source.activity_depth_unit,\n" + 
				"               result_no_source.activity_depth_ref_point,\n" + 
				"               result_no_source.activity_upper_depth,\n" + 
				"               result_no_source.activity_upper_depth_unit,\n" + 
				"               result_no_source.activity_lower_depth,\n" + 
				"               result_no_source.activity_lower_depth_unit,\n" + 
				"               result_no_source.project_id,\n" + 
				"               result_no_source.activity_conducting_org,\n" + 
				"               result_no_source.activity_comment,\n" + 
				"               result_no_source.sample_collect_method_id,\n" + 
				"               result_no_source.sample_collect_method_ctx,\n" + 
				"               result_no_source.sample_collect_method_name,\n" + 
				"               result_no_source.sample_collect_equip_name,\n" + 
				"               result_no_source.result_id,\n" + 
				"               result_no_source.result_detection_condition_tx,\n" + 
				"               result_no_source.sample_fraction_type,\n" + 
				"               result_no_source.result_measure_value,\n" + 
				"               result_no_source.result_unit,\n" + 
				"               result_no_source.result_meas_qual_code,\n" + 
				"               result_no_source.result_value_status,\n" + 
				"               result_no_source.statistic_type,\n" + 
				"               result_no_source.result_value_type,\n" + 
				"               result_no_source.weight_basis_type,\n" + 
				"               result_no_source.duration_basis,\n" + 
				"               result_no_source.temperature_basis_level,\n" + 
				"               result_no_source.particle_size,\n" + 
				"               result_no_source.precision,\n" + 
				"               result_no_source.result_comment,\n" + 
				"               result_no_source.result_depth_meas_value,\n" + 
				"               result_no_source.result_depth_meas_unit_code,\n" + 
				"               result_no_source.result_depth_alt_ref_pt_txt,\n" + 
				"               result_no_source.sample_tissue_taxonomic_name,\n" + 
				"               result_no_source.analytical_procedure_id,\n" + 
				"               result_no_source.analytical_procedure_source,\n" + 
				"               result_no_source.analytical_method_name,\n" + 
				"               result_no_source.lab_name,\n" + 
				"               result_no_source.analysis_date_time,\n" + 
				"               result_no_source.lab_remark,\n" + 
				"               result_no_source.detection_limit,\n" + 
				"               result_no_source.detection_limit_unit,\n" + 
				"               result_no_source.detection_limit_desc,\n" + 
				"               result_no_source.analysis_prep_date_tx\n" + 
				"          from result_no_source\n" + 
				"               join station_swap_storet station\n" + 
				"                 on result_no_source.station_id + 10000000 = station.station_id) a");
		return RepeatStatus.FINISHED;
	}
}
