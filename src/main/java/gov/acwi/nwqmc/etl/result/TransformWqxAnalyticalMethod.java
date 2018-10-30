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
public class TransformWqxAnalyticalMethod implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformWqxAnalyticalMethod(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into wqx_analytical_method (anlmth_uid, anlmth_id, amctx_cd, anlmth_name, anlmth_url, anlmth_qual_type, nemi_url)\n" + 
				"select /*+ parallel(4) */\n" + 
				"       analytical_method.anlmth_uid,\n" + 
				"       analytical_method.anlmth_id,\n" + 
				"       analytical_method_context.amctx_cd,\n" + 
				"       analytical_method.anlmth_name,\n" + 
				"       analytical_method.anlmth_url,\n" + 
				"       analytical_method.anlmth_qual_type,\n" + 
				"       wqp_nemi_epa_crosswalk.nemi_url\n" + 
				"  from wqx.analytical_method\n" + 
				"       left join wqx.analytical_method_context\n" + 
				"         on analytical_method.amctx_uid = analytical_method_context.amctx_uid\n" + 
				"       left join wqp_nemi_epa_crosswalk\n" + 
				"         on analytical_method_context.amctx_cd = wqp_nemi_epa_crosswalk.analytical_procedure_source and\n" + 
				"            analytical_method.anlmth_id = wqp_nemi_epa_crosswalk.analytical_procedure_id");
		return RepeatStatus.FINISHED;
	}
}
