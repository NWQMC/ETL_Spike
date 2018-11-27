package gov.acwi.wqp.etl;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import gov.acwi.wqp.etl.DbConfigX;

@JdbcTest
@AutoConfigureTestDatabase(replace=Replace.NONE)
@Transactional(propagation=Propagation.NOT_SUPPORTED)
@Import({DBTestConfig.class, DbConfigX.class})
public abstract class BaseJdbcIT extends BaseIT {

	@MockBean
	protected JobLauncher jobLauncher;
	@MockBean
	protected Job job;
	@Autowired
	protected JdbcTemplate jdbcTemplate;

}
