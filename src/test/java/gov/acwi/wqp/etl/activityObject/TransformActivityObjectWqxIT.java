package gov.acwi.wqp.etl.activityObject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.WqxBaseFlowIT;

public class TransformActivityObjectWqxIT extends WqxBaseFlowIT {

	@Test
	@DatabaseSetup(
			connection=WqxBaseFlowIT.CONNECTION_WQX_DUMP,
			value="classpath:/testData/wqxDump/csv/"
			)
	@DatabaseSetup(value="classpath:/testData/wqp/activityObject/storet/empty.xml")
	@DatabaseSetup(value="classpath:/testData/wqp/activity/storet/activity_swap_storet.xml")
	@ExpectedDatabase(
			value="classpath:/testData/wqp/activityObject/storet/activity_object_swap_storet.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table="activity_object_swap_storet",
			query="select data_source_id, object_id, data_source, organization, activity_id, activity, object_name, object_type, encode(object_content, 'escape') object_content from activity_object_swap_storet"
			)
	public void transformTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformActivityObjectWqxStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
