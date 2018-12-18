package gov.acwi.wqp.etl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Profile;

@SpringBootApplication
@Profile("default")
public class Application implements CommandLineRunner {
	private static final Logger LOG = LoggerFactory.getLogger(Application.class);

	public static final String JOB_ID = "jobId";
	public static final String DATASOURCE = "datasource";
	public static final String DATASOURCE_STORET = "STORET";

	@Autowired
	private Job job;
	@Autowired
	private JobIncrementer jobIncrementer;
	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobOperator jobOperator;
	@Autowired
	private JobExplorer jobExplorer;

	public static void main(String[] args) {
		LOG.info(args.toString());
		SpringApplication.run(Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		JobExecution jobExecution = null;
		try {
			if (!jobOperator.getRunningExecutions(job.getName()).isEmpty()) {
				LOG.info("This run cancelled, there is already a job running for " + job.getName());
			}
		} catch (NoSuchJobException e) {
			LOG.info("Attempting to restart " + job.getName());
			jobExecution = reStartJob();
		}
		if (null == jobExecution 
				|| ExitStatus.UNKNOWN.equals(jobExecution.getExitStatus())
				|| ExitStatus.FAILED.equals(jobExecution.getExitStatus())
				|| ExitStatus.STOPPED.equals(jobExecution.getExitStatus())) {
			throw new RuntimeException("Job did not complete as planned.");
		}
	}

	protected JobParametersBuilder getJobParametersBuilder() {
		return new JobParametersBuilder(jobExplorer)
				.addString(DATASOURCE, DATASOURCE_STORET, true);
	}

	protected JobExecution reStartJob() throws Exception {
		JobParameters parameters = getJobParametersBuilder()
				.addLong(JOB_ID, jobIncrementer.getCurrent(), true)
				.toJobParameters();
		try {
			return jobLauncher.run(job, parameters);
		} catch (JobInstanceAlreadyCompleteException e) {
			LOG.info("Job " + job.getName() + " #" + parameters.getString(JOB_ID) + " has already completed successfully.");
			return startNewJobInstance();
		}
	}

	protected JobExecution startNewJobInstance() throws Exception {
		JobParameters parameters = getJobParametersBuilder()
				.getNextJobParameters(job)
				.toJobParameters();
		LOG.info("Attempting to start run job " + job.getName() + " #" + parameters.getString(JOB_ID));
		return jobLauncher.run(job, parameters);
	}

}
