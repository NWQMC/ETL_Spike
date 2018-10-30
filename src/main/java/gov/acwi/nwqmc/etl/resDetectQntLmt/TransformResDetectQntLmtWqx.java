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
public class TransformResDetectQntLmtWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformResDetectQntLmtWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into r_detect_qnt_lmt_swap_storet(data_source_id, data_source, station_id, site_id, event_date, activity, analytical_method,\n" + 
				"                                    characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code,\n" + 
				"                                    organization_name, project_id, assemblage_sampled_name, sample_tissue_taxonomic_name, activity_id,\n" + 
				"                                    result_id, detection_limit_id, detection_limit, detection_limit_unit, detection_limit_desc)\n" + 
				"select /*+ parallel(4) */\n" + 
				"       result_swap_storet.data_source_id,\n" + 
				"       result_swap_storet.data_source,\n" + 
				"       result_swap_storet.station_id,\n" + 
				"       result_swap_storet.site_id,\n" + 
				"       result_swap_storet.event_date,\n" + 
				"       result_swap_storet.activity,\n" + 
				"       result_swap_storet.analytical_method,\n" + 
				"       result_swap_storet.characteristic_name,\n" + 
				"       result_swap_storet.characteristic_type,\n" + 
				"       result_swap_storet.sample_media,\n" + 
				"       result_swap_storet.organization,\n" + 
				"       result_swap_storet.site_type,\n" + 
				"       result_swap_storet.huc,\n" + 
				"       result_swap_storet.governmental_unit_code,\n" + 
				"       result_swap_storet.organization_name,\n" + 
				"       result_swap_storet.project_id,\n" + 
				"       result_swap_storet.assemblage_sampled_name,\n" + 
				"       result_swap_storet.sample_tissue_taxonomic_name,\n" + 
				"       result_swap_storet.activity_id,\n" + 
				"       result_swap_storet.result_id,\n" + 
				"       wqx_r_detect_qnt_lmt.rdqlmt_uid,\n" + 
				"       wqx_r_detect_qnt_lmt.rdqlmt_measure,\n" + 
				"       wqx_r_detect_qnt_lmt.msunt_cd,\n" + 
				"       wqx_r_detect_qnt_lmt.dqltyp_name\n" + 
				"  from wqx_r_detect_qnt_lmt\n" + 
				"       join result_swap_storet\n" + 
				"         on wqx_r_detect_qnt_lmt.res_uid = result_swap_storet.result_id");
		return RepeatStatus.FINISHED;
	}
}
