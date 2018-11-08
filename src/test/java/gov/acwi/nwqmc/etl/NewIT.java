package gov.acwi.nwqmc.etl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

public class NewIT extends BaseStepIT {

	@Before
	public void clearMetadata() {
		jobRepositoryTestUtils.removeJobExecutions();
	}

	@Test
	public void endToEndTest() throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.launchJob(getJobParameters());
		Assert.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
	}
}