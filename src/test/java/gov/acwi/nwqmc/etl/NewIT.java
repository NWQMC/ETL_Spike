package gov.acwi.nwqmc.etl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
//import org.springframework.batch.core.step.tasklet.Tasklet;
//import org.springframework.batch.test.JobLauncherTestUtils;
//import org.springframework.batch.test.JobRepositoryTestUtils;
//import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import gov.acwi.nwqmc.etl.orgData.DropOrgDataIndexes;

//@SpringBatchTest
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestJobConfiguration.class})
public abstract class NewIT {
//	@Autowired
//	private JobLauncherTestUtils jobLauncherTestUtils;
//	@Autowired
//	private JobRepositoryTestUtils jobRepositoryTestUtils;
//	@Autowired
//	private JdbcTemplate jdbcTemplate;
//
//	@Before
//	public void clearMetadata() {
//		//TODO - maybe once per suite execution?
//		jobRepositoryTestUtils.removeJobExecutions();
//	}
//
//	@Test
//	public void givenTaskletsJob_WhenJobEnds_ThenStatusCompleted() throws Exception {
//		JobParameters jobParameters = new JobParametersBuilder()
//				.addJobParameters(jobLauncherTestUtils.getUniqueJobParameters())
//				.addString("datasource", "STORET")
//				.toJobParameters();
//
////		JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
//		JobExecution jobExecution = jobLauncherTestUtils.launchStep("truncateOrgDataStep", jobParameters);
//		Assert.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
//	}
//
//	@Test
//	public void help() throws Exception {
//		Tasklet dropOrgDataIndexes = new DropOrgDataIndexes(jdbcTemplate, "storet");
//		dropOrgDataIndexes.execute(null, null);
//	}

}