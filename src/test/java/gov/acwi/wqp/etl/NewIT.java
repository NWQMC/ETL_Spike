package gov.acwi.wqp.etl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;

@DbUnitConfiguration(dataSetLoader=CsvDataSetLoader.class)
public class NewIT extends BaseStepIT {

	@Before
	public void clearMetadata() {
		jobRepositoryTestUtils.removeJobExecutions();
	}

	@Test
	@DatabaseSetup(value="classpath:/testData/wqp/etlThreshold/")
	public void endToEndTest() throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.launchJob(getJobParameters());
		Assert.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
	}
}