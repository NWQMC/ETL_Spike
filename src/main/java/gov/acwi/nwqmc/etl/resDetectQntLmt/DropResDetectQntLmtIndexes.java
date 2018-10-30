package gov.acwi.nwqmc.etl.resDetectQntLmt;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@StepScope
public class DropResDetectQntLmtIndexes implements Tasklet, InitializingBean {

	private final JdbcTemplate jdbcTemplate;

	@Value("#{jobParameters['datasource']}")
	String datasource;

	@Autowired
	public DropResDetectQntLmtIndexes(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution,
			ChunkContext chunkContext) throws Exception {
		jdbcTemplate.update("call etl_helper_r_detect_qnt_lmt.drop_indexes(?)", datasource);
		return RepeatStatus.FINISHED;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// TODO Auto-generated method stub
	}
}
