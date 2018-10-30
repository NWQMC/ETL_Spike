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
public class TransformWqxRDetectQntLmt implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformWqxRDetectQntLmt(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into wqx_r_detect_qnt_lmt (res_uid, rdqlmt_uid, rdqlmt_measure, msunt_cd, dqltyp_uid, dqltyp_name)\n" + 
				"select /*+ parallel(4) */\n" + 
				"       result_detect_quant_limit.res_uid,\n" + 
				"       result_detect_quant_limit.rdqlmt_uid,\n" + 
				"       result_detect_quant_limit.rdqlmt_measure,\n" + 
				"       measurement_unit.msunt_cd,\n" + 
				"       result_detect_quant_limit.dqltyp_uid,\n" + 
				"       detection_quant_limit_type.dqltyp_name\n" + 
				"  from wqx.result_detect_quant_limit\n" + 
				"       left join wqx.measurement_unit\n" + 
				"         on result_detect_quant_limit.msunt_uid = measurement_unit.msunt_uid\n" + 
				"       left join wqx.detection_quant_limit_type\n" + 
				"         on result_detect_quant_limit.dqltyp_uid = detection_quant_limit_type.dqltyp_uid");
		return RepeatStatus.FINISHED;
	}
}
