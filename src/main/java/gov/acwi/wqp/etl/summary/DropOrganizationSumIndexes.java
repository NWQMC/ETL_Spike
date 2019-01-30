package gov.acwi.wqp.etl.summary;

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
public class DropOrganizationSumIndexes implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public DropOrganizationSumIndexes(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.update("call etl_helper_main.drop_indexes('ORGANIZATION_SUM_SWAP_STORET')");
		return RepeatStatus.FINISHED;
	}
}
