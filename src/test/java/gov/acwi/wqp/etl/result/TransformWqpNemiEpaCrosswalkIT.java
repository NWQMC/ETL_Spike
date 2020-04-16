package gov.acwi.wqp.etl.result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.WqxBaseFlowIT;

@Disabled
public class TransformWqpNemiEpaCrosswalkIT extends WqxBaseFlowIT {

	@Test
	@DatabaseSetup(value="classpath:/testData/wqp/wqpNemiEpaCrosswalk/empty.xml")
	@ExpectedDatabase(value="classpath:/testData/wqp/wqpNemiEpaCrosswalk/wqpNemiEpaCrosswalk.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void transformTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformWqpNemiEpaCrosswalkStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
