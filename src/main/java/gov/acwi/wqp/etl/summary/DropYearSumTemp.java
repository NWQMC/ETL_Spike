package gov.acwi.wqp.etl.summary;

import java.sql.SQLSyntaxErrorException;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@StepScope
public class DropYearSumTemp implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public DropYearSumTemp(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		try {
			jdbcTemplate.update("drop table year_sum_temp_storet cascade constraints purge");
		} catch (DataAccessException e) {
			if (e.getRootCause() instanceof SQLSyntaxErrorException) {
				SQLSyntaxErrorException rootCause = (SQLSyntaxErrorException) e.getRootCause();
				if (rootCause.getErrorCode() != 942) {
					throw e;
				}
			}
		}
		return RepeatStatus.FINISHED;
	}
}
