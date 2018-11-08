package gov.acwi.nwqmc.etl.projectData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.repeat.RepeatStatus;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.nwqmc.etl.BaseJdbcIT;

//TODO - currently fails due to enable RI
public abstract class TruncateProjectDataIT extends BaseJdbcIT {

	@Test
	@DatabaseSetup(value="classpath:/testData/wqp/projectData/storet/project_data_swap_storet.xml")
	@ExpectedDatabase(value="classpath:/testData/wqp/projectData/storet/empty.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void truncateTest() {
		TruncateProjectData truncateProject = new TruncateProjectData(jdbcTemplate);
		try {
			assertEquals(RepeatStatus.FINISHED, truncateProject.execute(null, null));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
}
