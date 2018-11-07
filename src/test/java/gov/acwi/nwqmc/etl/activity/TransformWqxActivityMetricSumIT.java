package gov.acwi.nwqmc.etl.activity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.repeat.RepeatStatus;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.nwqmc.etl.BaseIT;

public abstract class TransformWqxActivityMetricSumIT extends BaseIT {

	@Test
	@DatabaseSetup(value="classpath:/testData/wqp/wqxActivityMetricSum/wqxActivityMetricSum.xml")
	@ExpectedDatabase(value="classpath:/testData/wqp/wqxActivityMetricSum/empty.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void truncateTest() {
		TransformWqxActivityMetricSum transformWqxActivityMetricSum = new TransformWqxActivityMetricSum(jdbcTemplate);
		try {
			assertEquals(RepeatStatus.FINISHED, transformWqxActivityMetricSum.execute(null, null));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		fail("need to implement");
	}
}
