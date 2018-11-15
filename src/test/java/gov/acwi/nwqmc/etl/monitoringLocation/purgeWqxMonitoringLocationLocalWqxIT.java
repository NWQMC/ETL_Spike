package gov.acwi.nwqmc.etl.monitoringLocation;

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

public class purgeWqxMonitoringLocationLocalWqxIT extends BaseStepIT {

	@Test
	@DatabaseSetup(value="classpath:/testData/wqp/monitoringLocation/wqxStationLocal/storet/wqx_station_local.xml")
	//TODO Handle GEOM & better test data (select bits from geom for compare OR override geom to be comparable)
	@ExpectedDatabase(table="wqx_station_local",
			query="select station_source, station_id, site_id, latitude, longitude, hrdat_uid, huc, cntry_cd, st_fips_cd, cnty_fips_cd, calculated_huc_12, calculated_fips from wqx_station_local",
			value="classpath:/testData/wqp/monitoringLocation/wqxStationLocal/storet/wqx_station_local_purged_wqx.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void transformTest() {
		JobParameters jobParameters = new JobParametersBuilder()
			.addJobParameters(jobLauncherTestUtils.getUniqueJobParameters())
			.addString("datasource", "STORET")
			.toJobParameters();
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("purgeWqxMonitoringLocationLocalWqxStep", jobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
