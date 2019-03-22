package gov.acwi.wqp.etl;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
	private JobLauncher jobLauncher;
	@Autowired
	private JobOperator jobOperator;
	@Autowired
	private JobExplorer jobExplorer;

	// Load date as a system property because it contains spaces
	@Value("${app.job.id.date}")
	private String jobIdDate;

	public static void main(String[] args) {
		LOG.info(args.toString());
		SpringApplication.run(Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// probe the args to see if any are being passed into the app
		// Why, you say? Because the args have not been used until now.
		LOG.info("Batch execution with parameter arguments " + Arrays.asList(args));
		// check the system parameter value also
		LOG.info("Batch execution job id date '" + jobIdDate + "'");

		ExitStatus exitStatus = null;
		try {
			if (!jobOperator.getRunningExecutions(job.getName()).isEmpty()) {
				LOG.info("This run cancelled, there is already a job running for " + job.getName());
			}
		} catch (NoSuchJobException e) {
			// OUCH! THis is the happy path controlled by an exception.
			LOG.info("Attempting to restart " + job.getName());
			exitStatus = startJob(jobIdDate);
		}
		if (null == exitStatus
				|| ExitStatus.UNKNOWN.getExitCode().contentEquals(exitStatus.getExitCode())
				|| ExitStatus.FAILED.getExitCode().contentEquals(exitStatus.getExitCode())
				|| ExitStatus.STOPPED.getExitCode().contentEquals(exitStatus.getExitCode())) {
			throw new RuntimeException("Job did not complete as planned.");
		}
	}

	protected JobParameters getJobParametersBuilder(String jobIdDate) {
		return new JobParametersBuilder(jobExplorer)
				.addString(DATASOURCE, DATASOURCE_STORET, true)
				// this is the new date based job ID
				.addString(JOB_ID, jobIdDate, true)
				.toJobParameters();
	}

	protected ExitStatus startJob(String jobIdDate) throws Exception {
		JobParameters parameters = getJobParametersBuilder(jobIdDate);

		try {
			return jobLauncher.run(job, parameters).getExitStatus();
		} catch (JobInstanceAlreadyCompleteException e) {
			// log the new date job ID
			LOG.info("Job " + job.getName() + " for '" + parameters.getString(JOB_ID) + "' has already completed successfully.");
			return ExitStatus.COMPLETED; // If it was already done then things are A.O.K.
		}
	}
}
