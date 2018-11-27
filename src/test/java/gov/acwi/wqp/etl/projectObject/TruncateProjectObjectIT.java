package gov.acwi.wqp.etl.projectObject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.repeat.RepeatStatus;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.BaseJdbcIT;
import gov.acwi.wqp.etl.projectObject.TruncateProjectObject;

public class TruncateProjectObjectIT extends BaseJdbcIT {

	@Test
	@DatabaseSetup(value="classpath:/testData/wqp/projectObject/storet/project_object_swap_storet.xml")
	@ExpectedDatabase(value="classpath:/testData/wqp/projectObject/storet/empty.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void truncateTest() {
		TruncateProjectObject truncateObject = new TruncateProjectObject(jdbcTemplate);
		try {
			assertEquals(RepeatStatus.FINISHED, truncateObject.execute(null, null));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
