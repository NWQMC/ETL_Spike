package gov.acwi.nwqmc.etl.orgData;

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

import gov.acwi.nwqmc.etl.BaseStepIT;

public class TransformOrgDataWqxIT extends BaseStepIT {

	@Test
	@DatabaseSetup(value="classpath:/testData/wqp/orgData/storet/empty.xml")
	@ExpectedDatabase(value="classpath:/testData/wqp/orgData/storet/org_data_swap_storet.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void transformTest() {
		JobParameters jobParameters = new JobParametersBuilder()
			.addJobParameters(jobLauncherTestUtils.getUniqueJobParameters())
			.addString("datasource", "STORET")
			.toJobParameters();
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformOrgDataWqxStep", jobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
