package gov.acwi.wqp.etl.projectObject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.WqxBaseFlowIT;

public class TransformProjectObjectWqxIT extends WqxBaseFlowIT {

	@Test
	@DatabaseSetup(
			connection=WqxBaseFlowIT.CONNECTION_WQX_DUMP,
			value="classpath:/testData/wqxDump/csv/"
			)
	@DatabaseSetup(value="classpath:/testData/wqp/projectObject/storet/empty.xml")
	@ExpectedDatabase(
			value="classpath:/testData/wqp/projectObject/storet/project_object_swap_storet.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table="project_object_swap_storet",
			query="select data_source_id, object_id, data_source, organization, project_identifier, object_name, object_type, encode(object_content, 'escape') object_content from project_object_swap_storet"
			)
	public void transformTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformProjectObjectWqxStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
