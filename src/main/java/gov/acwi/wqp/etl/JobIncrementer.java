package gov.acwi.wqp.etl;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class JobIncrementer implements JobParametersIncrementer {
	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public JobIncrementer(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public JobParameters getNext(JobParameters parameters) {
		Long id = jdbcTemplate.queryForObject("select wqx_etl_job_instance.nextval from dual", Long.class);
		jdbcTemplate.update("update wqx_etl_current_job set id = ?", id);
		return new JobParametersBuilder().addLong(Application.JOB_ID, id).toJobParameters();
	}

	public long getCurrent() {
		return jdbcTemplate.queryForObject("select id from wqx_etl_current_job", Long.class);
	}
}
