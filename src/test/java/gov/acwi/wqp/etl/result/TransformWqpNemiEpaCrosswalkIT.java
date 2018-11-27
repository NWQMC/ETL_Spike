package gov.acwi.wqp.etl.result;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.Application;
import gov.acwi.wqp.etl.BaseStepIT;

public class TransformWqpNemiEpaCrosswalkIT extends BaseStepIT {

	@Test
	@DatabaseSetup(value="classpath:/testData/wqp/wqpNemiEpaCrosswalk/empty.xml")
	@ExpectedDatabase(value="classpath:/testData/wqp/wqpNemiEpaCrosswalk/wqpNemiEpaCrosswalk.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void transformTest() {
		JobParameters jobParameters = new JobParametersBuilder()
			.addJobParameters(jobLauncherTestUtils.getUniqueJobParameters())
			.addString(Application.DATASOURCE, Application.DATASOURCE_STORET)
			.toJobParameters();
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformWqpNemiEpaCrosswalkStep", jobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
