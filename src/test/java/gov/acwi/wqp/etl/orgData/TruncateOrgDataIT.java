package gov.acwi.wqp.etl.orgData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.repeat.RepeatStatus;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.BaseJdbcIT;
import gov.acwi.wqp.etl.orgData.TruncateOrgData;

public class TruncateOrgDataIT extends BaseJdbcIT {

	@Test
	@DatabaseSetup(value="classpath:/testData/wqp/orgData/storet/org_data_swap_storet.xml")
	@ExpectedDatabase(value="classpath:/testData/wqp/orgData/storet/empty.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void truncateTest() {
		TruncateOrgData truncateOrgData = new TruncateOrgData(jdbcTemplate);
		try {
			assertEquals(RepeatStatus.FINISHED, truncateOrgData.execute(null, null));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
