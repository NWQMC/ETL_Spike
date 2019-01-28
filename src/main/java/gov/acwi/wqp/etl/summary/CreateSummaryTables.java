package gov.acwi.wqp.etl.summary;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

@Component
@StepScope
public class CreateSummaryTables implements Tasklet {

	private final JdbcTemplate jdbcTemplate;
	private final String datasource;

	@Value("classpath:sql/summary/organizationSum.sql")
	private Resource resource;

	@Autowired
	public CreateSummaryTables(JdbcTemplate jdbcTemplate,
			@Value("#{jobParameters['datasource']}") String datasource) {
		this.jdbcTemplate = jdbcTemplate;
		this.datasource = datasource;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		//jdbcTemplate.update("call etl_helper_summary.create_tables(?)", datasource);
		jdbcTemplate.update("call etl_helper_summary.create_activity_sum(?)", datasource);
		jdbcTemplate.update("call etl_helper_summary.create_result_sum(?)", datasource);
		jdbcTemplate.update("call etl_helper_summary.create_org_grouping(?)", datasource);
		jdbcTemplate.update("call etl_helper_summary.create_ml_grouping(?)", datasource);

		String sql = new String(FileCopyUtils.copyToByteArray(resource.getInputStream()));
		jdbcTemplate.execute(sql);

		jdbcTemplate.update("call etl_helper_summary.create_station_sum(?)", datasource);
		jdbcTemplate.update("call etl_helper_summary.create_qwportal_summary(?)", datasource);
		return RepeatStatus.FINISHED;
	}
}
