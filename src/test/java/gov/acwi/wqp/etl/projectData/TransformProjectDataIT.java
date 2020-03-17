package gov.acwi.wqp.etl.projectData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
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

public class TransformProjectDataIT extends WqxBaseFlowIT {

	public static final String TABLE_NAME = "'project_data_swap_storet'";
	public static final String EXPECTED_DATABASE_QUERY_ANALYZE = BASE_EXPECTED_DATABASE_QUERY_ANALYZE + TABLE_NAME;
	public static final String EXPECTED_DATABASE_QUERY_PRIMARY_KEY = BASE_EXPECTED_DATABASE_QUERY_PRIMARY_KEY
			+ EQUALS_QUERY + TABLE_NAME;
	public static final String EXPECTED_DATABASE_QUERY_FOREIGN_KEY = BASE_EXPECTED_DATABASE_QUERY_FOREIGN_KEY
			+ EQUALS_QUERY + TABLE_NAME;

	@Autowired
	@Qualifier("projectDataFlow")
	private Flow projectDataFlow;

	private Job setupFlowTestJob() {
		return jobBuilderFactory.get("projectDataFlowTest").start(projectDataFlow).build().build();
	}

	//TODO - WQP-1416
//	@Test
//	@DatabaseSetup(value="classpath:/testResult/storet/projectData/empty.xml")
//	@DatabaseSetup(connection=CONNECTION_ARS, value="classpath:/testResult/ars/arsOrgProject/arsOrgProject.xml")
//	@ExpectedDatabase(value="classpath:/testResult/storet/projectData/projectData.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	public void transformProjectDataStepTest() {
//		jobLauncherTestUtils.setJob(setupFlowTestJob());
//		try {
//			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformProjectDataStep", testJobParameters);
//			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
//		} catch (Exception e) {
//			e.printStackTrace();
//			fail(e.getLocalizedMessage());
//		}
//	}

	@Test
	//TODO - WQP-1416
//	@DatabaseSetup(value="classpath:/testData/storet/projectData/projectDataOld.xml")
//	@DatabaseSetup(connection=CONNECTION_ARS, value="classpath:/testResult/ars/arsOrgProject/arsOrgProject.xml")
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/storetw/projectDataNoSource/csv/"
			)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/projectData/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/projectData/indexes/all.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_INDEX,
			query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX + TABLE_NAME)
	@ExpectedDatabase(
			connection=CONNECTION_INFORMATION_SCHEMA,
			value="classpath:/testResult/storet/projectData/create.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_TABLE,
			query=BASE_EXPECTED_DATABASE_QUERY_CHECK_TABLE + TABLE_NAME)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/analyze/projectData.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_ANALYZE,
			query=EXPECTED_DATABASE_QUERY_ANALYZE)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/projectData/primaryKey.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_PRIMARY_KEY,
			query=EXPECTED_DATABASE_QUERY_PRIMARY_KEY)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/projectData/foreignKey.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_FOREIGN_KEY,
			query=EXPECTED_DATABASE_QUERY_FOREIGN_KEY)
	@ExpectedDatabase(
			value="classpath:/testResult/storet/projectData/indexes/pk.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_INDEX,
			query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX_PK + TABLE_NAME)
	public void projectDataFlowTest() {
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
