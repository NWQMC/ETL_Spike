package gov.acwi.nwqmc.etl.resDetectQntLmt;

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
public class TransformResDetectQntLmtStoretw implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformResDetectQntLmtStoretw(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into r_detect_qnt_lmt_swap_storet(data_source_id, data_source, station_id, site_id, event_date, activity, analytical_method,\n" + 
				"                                    characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code,\n" + 
				"                                    organization_name, project_id, assemblage_sampled_name, sample_tissue_taxonomic_name, activity_id,\n" + 
				"                                    result_id, detection_limit_id, detection_limit, detection_limit_unit, detection_limit_desc)\n" + 
				"select 3 data_source_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       a.*\n" + 
				"  from (select /*+ parallel(4) */\n" + 
				"               station.station_id,\n" + 
				"               station.site_id,\n" + 
				"               result_no_source.event_date,\n" + 
				"               result_no_source.activity,\n" + 
				"               result_no_source.analytical_method,\n" + 
				"               result_no_source.characteristic_name,\n" + 
				"               result_no_source.characteristic_type,\n" + 
				"               result_no_source.sample_media,\n" + 
				"               station.organization,\n" + 
				"               station.site_type,\n" + 
				"               station.huc,\n" + 
				"               station.governmental_unit_code,\n" + 
				"               station.organization_name,\n" + 
				"               result_no_source.project_id,\n" + 
				"               null assemblage_sampled_name,\n" + 
				"               result_no_source.sample_tissue_taxonomic_name,\n" + 
				"               result_no_source.activity_id,\n" + 
				"               result_no_source.result_id,\n" + 
				"               result_no_source.result_id detection_limit_id,\n" + 
				"               result_no_source.detection_limit,\n" + 
				"               result_no_source.detection_limit_unit,\n" + 
				"               result_no_source.detection_limit_desc\n" + 
				"          from result_no_source\n" + 
				"               join station_swap_storet station\n" + 
				"                 on result_no_source.station_id + 10000000 = station.station_id) a");
		return RepeatStatus.FINISHED;
	}
}
