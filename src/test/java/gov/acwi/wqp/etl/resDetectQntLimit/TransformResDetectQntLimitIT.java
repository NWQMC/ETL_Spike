package gov.acwi.wqp.etl.resDetectQntLimit;

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

public class TransformResDetectQntLimitIT extends WqxBaseFlowIT {

	public static final String TABLE_NAME = "'r_detect_qnt_lmt_swap_storet'";
	public static final String EXPECTED_DATABASE_QUERY_ANALYZE = BASE_EXPECTED_DATABASE_QUERY_ANALYZE + TABLE_NAME;

	@Autowired
	@Qualifier("resDetectQntLimitFlow")
	private Flow resDetectQntLimitFlow;

	private Job setupFlowTestJob() {
		return jobBuilderFactory.get("resDetectQntLimitFlowTest").start(resDetectQntLimitFlow).build().build();
	}

	//TODO - WQP-1428
//	@Test
//	@DatabaseSetup(value="classpath:/testResult/storet/resDetectQntLimit/empty.xml")
//	@DatabaseSetup(value="classpath:/testResult/storet/result/result.xml")
//	@ExpectedDatabase(value="classpath:/testResult/storet/resDetectQntLimit/resDetectQntLimit.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	public void transformResDetectQntLimitStepTest() {
//		jobLauncherTestUtils.setJob(setupFlowTestJob());
//		try {
//			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformResDetectQntLimitStep", testJobParameters);
//			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
//		} catch (Exception e) {
//			e.printStackTrace();
//			fail(e.getLocalizedMessage());
//		}
//	}

	@Test
	//TODO - WQP-1428
//	@DatabaseSetup(value="classpath:/testData/storet/resDetectQntLimit/resDetectQntLimitOld.xml")
	@DatabaseSetup(
			value="classpath:/testResult/storet/monitoringLocation/csv/"
			)
//	@DatabaseSetup(value="classpath:/testResult/storet/activity/activity.xml")
//	@DatabaseSetup(value="classpath:/testResult/storet/result/result.xml")
	@DatabaseSetup(
			connection=CONNECTION_WQX,
			value="classpath:/testData/wqx/rDectQntLmt/csv/"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/storetw/resultNoSource/csv/"
			)
	@DatabaseSetup(
			value="classpath:testResult/storet/result/csv/"
			)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/resDetectQntLimit/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/resDetectQntLimit/indexes/all.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_INDEX,
			query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX + TABLE_NAME)
	@ExpectedDatabase(
			connection=CONNECTION_INFORMATION_SCHEMA,
			value="classpath:/testResult/storet/resDetectQntLimit/create.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_TABLE,
			query=BASE_EXPECTED_DATABASE_QUERY_CHECK_TABLE + TABLE_NAME)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/analyze/resDetectQntLimit.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_ANALYZE,
			query=EXPECTED_DATABASE_QUERY_ANALYZE)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/analyze/resDetectQntLimit.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_ANALYZE,
			query=EXPECTED_DATABASE_QUERY_ANALYZE)
	public void resDetectQntLimitFlowTest() {
		jobLauncherTestUtils.setJob(setupFlowTestJob());
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
