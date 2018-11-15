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
public class TruncateResDetectQntLmt implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TruncateResDetectQntLmt(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("truncate table r_detect_qnt_lmt_swap_storet");
		return RepeatStatus.FINISHED;
	}
}
