package gov.acwi.wqp.etl.result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.WqxBaseFlowIT;

public class TransformResultIT extends WqxBaseFlowIT {

	public static final String TABLE_NAME = "'result_swap_storet'";
	public static final String EXPECTED_DATABASE_QUERY_ANALYZE = BASE_EXPECTED_DATABASE_QUERY_ANALYZE + TABLE_NAME;
	public static final String EXPECTED_DATABASE_QUERY_PRIMARY_KEY = BASE_EXPECTED_DATABASE_QUERY_PRIMARY_KEY
			+ EQUALS_QUERY + TABLE_NAME;
	public static final String EXPECTED_DATABASE_QUERY_FOREIGN_KEY = BASE_EXPECTED_DATABASE_QUERY_FOREIGN_KEY
			+ EQUALS_QUERY + TABLE_NAME;

	@Autowired
	@Qualifier("resultFlow")
	private Flow resultFlow;

	private Job setupFlowTestJob() {
		return jobBuilderFactory.get("resultFlowTest").start(resultFlow).build().build();
	}

	//TODO - WQP-1426
//	@Test
//	@DatabaseSetup(value="classpath:/testResult/storet/result/empty.xml")
//	@DatabaseSetup(value="classpath:/testResult/storet/activity/activity.xml")
//	@DatabaseSetup(connection=CONNECTION_ARS, value="classpath:/testResult/ars/arsResult/arsResult.xml")
//	@DatabaseSetup(connection=CONNECTION_ARS, value="classpath:/testData/ars/charNameToType.xml")
//	@ExpectedDatabase(value="classpath:/testResult/storet/result/result.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	public void transformResultStepTest() {
//		jobLauncherTestUtils.setJob(setupFlowTestJob());
//		try {
//			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformResultStep", testJobParameters);
//			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
//		} catch (Exception e) {
//			e.printStackTrace();
//			fail(e.getLocalizedMessage());
//		}
//	}

	@Test
	//TODO - WQP-1426
//	@DatabaseSetup(value="classpath:/testData/storet/result/resultOld.xml")
//	@DatabaseSetup(value="classpath:/testResult/storet/activity/activity.xml")
	@DatabaseSetup(
			value="classpath:/testResult/storet/monitoringLocation/csv/"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/storetw/resultNoSource/csv/"
			)
	@DatabaseSetup(
			connection=CONNECTION_WQP,
			value="classpath:/testResult/storet/activity/csv/"
			)
//	@DatabaseSetup(connection=CONNECTION_ARS, value="classpath:/testResult/ars/arsResult/arsResult.xml")
//	@DatabaseSetup(connection=CONNECTION_ARS, value="classpath:/testData/ars/charNameToType.xml")
	@ExpectedDatabase(
			value="classpath:/testResult/storet/result/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	@DatabaseSetup(
			connection=CONNECTION_WQX_DUMP,
			value="classpath:/testData/wqxDump/csv/"
	)
	@ExpectedDatabase(
			connection=CONNECTION_INFORMATION_SCHEMA,
			value="classpath:/testResult/storet/result/create.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_TABLE,
			query=BASE_EXPECTED_DATABASE_QUERY_CHECK_TABLE + TABLE_NAME)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/result/indexes/all.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_INDEX
			, query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX + TABLE_NAME)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/analyze/result.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_ANALYZE,
			query=EXPECTED_DATABASE_QUERY_ANALYZE)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/result/primaryKey.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_PRIMARY_KEY,
			query=EXPECTED_DATABASE_QUERY_PRIMARY_KEY)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/result/foreignKey.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_FOREIGN_KEY,
			query=EXPECTED_DATABASE_QUERY_FOREIGN_KEY)
	public void resultFlowTest() {
		jobLauncherTestUtils.setJob(setupFlowTestJob());
		jdbcTemplate.execute("select add_monitoring_location_primary_key('storet', 'wqp', 'station')");
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
