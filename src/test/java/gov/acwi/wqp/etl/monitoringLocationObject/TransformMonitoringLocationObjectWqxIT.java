package gov.acwi.wqp.etl.monitoringLocationObject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.WqxBaseFlowIT;

public class TransformMonitoringLocationObjectWqxIT extends WqxBaseFlowIT {

	@Test
	@DatabaseSetup(
			connection=WqxBaseFlowIT.CONNECTION_WQX_DUMP,
			value="classpath:/testData/wqxDump/csv/"
			)
	@DatabaseSetup(value="classpath:/testData/wqp/monitoringLocationObject/storet/empty.xml")
	@DatabaseSetup(value="classpath:/testData/wqp/monitoringLocation/storet/monitoring_location_swap_storet.xml")
	@ExpectedDatabase(
			value="classpath:/testData/wqp/monitoringLocationObject/storet/monitoring_location_object_swap_storet.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table="station_object_swap_storet",
			query="select data_source_id, object_id, data_source, organization, station_id, site_id, object_name, object_type, encode(object_content, 'escape') object_content from station_object_swap_storet"
			)
	public void transformTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformMonitoringLocationObjectWqxStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
