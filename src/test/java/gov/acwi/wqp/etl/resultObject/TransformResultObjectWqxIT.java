package gov.acwi.wqp.etl.resultObject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.WqxBaseFlowIT;

public class TransformResultObjectWqxIT extends WqxBaseFlowIT {

	@Test
	@DatabaseSetup(
			connection=CONNECTION_WQP,
			value="classpath:/testData/wqp/huc12nometa/"
			)
	@DatabaseSetup(
			connection=WqxBaseFlowIT.CONNECTION_WQX_DUMP,
			value="classpath:/testData/wqxDump/csv/"
			)
	@DatabaseSetup(value="classpath:/testData/wqp/resultObject/storet/empty.xml")
	@DatabaseSetup(value="classpath:/testData/wqp/result/storet/result_swap_storet.xml")
	@ExpectedDatabase(
			value="classpath:/testData/wqp/resultObject/storet/result_object_swap_storet.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table="result_object_swap_storet",
			query="select data_source_id, object_id, data_source, organization, activity_id, activity, result_id, object_name, object_type, encode(object_content, 'escape') object_content from result_object_swap_storet"
			)
	public void transformTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformResultObjectWqxStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
