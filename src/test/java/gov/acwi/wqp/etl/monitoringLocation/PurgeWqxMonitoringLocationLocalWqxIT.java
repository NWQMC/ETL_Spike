package gov.acwi.wqp.etl.monitoringLocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.WqxBaseFlowIT;

public class PurgeWqxMonitoringLocationLocalWqxIT extends WqxBaseFlowIT {

	@Test
	@DatabaseSetup(
			connection=CONNECTION_WQP,
			value="classpath:/testData/wqp/huc12nometa/"
			)
	@DatabaseSetup(
			connection=WqxBaseFlowIT.CONNECTION_WQX_DUMP,
			value="classpath:/testData/wqxDump/csv/"
			)
	@DatabaseSetup(
			connection=WqxBaseFlowIT.CONNECTION_WQX,
			value="classpath:/testData/wqx/monitoringLocationLocal/monitoring_location_local.xml"
			)
	//TODO Handle GEOM & better test data (select bits from geom for compare OR override geom to be comparable)
	@ExpectedDatabase(
			connection=WqxBaseFlowIT.CONNECTION_WQX,
			table="monitoring_location_local",
			query="select monitoring_location_source, station_id, site_id, latitude, longitude, hrdat_uid, huc, cntry_cd, st_fips_cd, cnty_fips_cd, calculated_huc_12, calculated_fips, geom from monitoring_location_local",
			value="classpath:/testResult/wqx/monitoringLocationLocal/purged_wqx.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	public void transformTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("purgeWqxMonitoringLocationLocalWqxStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
