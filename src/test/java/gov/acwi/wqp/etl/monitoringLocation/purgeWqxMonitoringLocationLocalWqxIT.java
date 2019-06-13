package gov.acwi.wqp.etl.monitoringLocation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.WqxBaseFlowIT;

public class purgeWqxMonitoringLocationLocalWqxIT extends WqxBaseFlowIT {

	@Test
	@DatabaseSetup(value="classpath:/testData/wqp/monitoringLocation/wqxStationLocal/storet/wqx_station_local.xml")
	//TODO Handle GEOM & better test data (select bits from geom for compare OR override geom to be comparable)
	@ExpectedDatabase(table="wqx_station_local",
			query="select station_source, station_id, site_id, latitude, longitude, hrdat_uid, huc, cntry_cd, st_fips_cd, cnty_fips_cd, calculated_huc_12, calculated_fips from wqx_station_local",
			value="classpath:/testData/wqp/monitoringLocation/wqxStationLocal/storet/wqx_station_local_purged_wqx.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
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
