package gov.acwi.wqp.etl.activity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.repeat.RepeatStatus;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.BaseJdbcIT;
import gov.acwi.wqp.etl.activity.TransformWqxActivityMetricSum;

//TODO
public abstract class TransformWqxActivityMetricSumIT extends BaseJdbcIT {

	@Test
	@DatabaseSetup(value="classpath:/testData/wqp/wqxActivityMetricSum/wqxActivityMetricSum.xml")
	@ExpectedDatabase(value="classpath:/testData/wqp/wqxActivityMetricSum/empty.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void truncateTest() {
		TransformWqxActivityMetricSum transformWqxActivityMetricSum = new TransformWqxActivityMetricSum(jdbcTemplate);
		try {
			assertEquals(RepeatStatus.FINISHED, transformWqxActivityMetricSum.execute(null, null));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
