package gov.acwi.nwqmc.etl.projectData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.github.springtestdbunit.TransactionDbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.nwqmc.etl.ColumnSensingFlatXMLDataSetLoader;
import gov.acwi.nwqmc.etl.TestJobConfiguration;

//@SpringBatchTest
//@RunWith(SpringRunner.class)
//@ActiveProfiles("it")
//@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class,
//	DirtiesContextTestExecutionListener.class,
//	TransactionalTestExecutionListener.class,
//	TransactionDbUnitTestExecutionListener.class })
//@DbUnitConfiguration(dataSetLoader=ColumnSensingFlatXMLDataSetLoader.class)
//@AutoConfigureTestDatabase(replace=Replace.NONE)
//@Transactional(propagation=Propagation.NOT_SUPPORTED)
//@SpringBootTest(classes = {TestJobConfiguration.class})
public abstract class TransformProjectDataWqxIT { // extends BaseIT {

	@Autowired
	protected JdbcTemplate jdbcTemplate;
	@Autowired
	private JobLauncherTestUtils jobLauncherTestUtils;

	@Test
	@DatabaseSetup(value="classpath:/testData/wqp/orgData/storet/empty.xml")
	@ExpectedDatabase(value="classpath:/testData/wqp/orgData/storet/org_data_swap_storet.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void transformTest() {
		JobParameters jobParameters = new JobParametersBuilder()
		.addJobParameters(jobLauncherTestUtils.getUniqueJobParameters())
		.addString("datasource", "STORET")
		.toJobParameters();
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformOrgDataWqxStep", jobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

}
