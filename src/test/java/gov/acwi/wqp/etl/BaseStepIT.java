package gov.acwi.wqp.etl;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigurationExcludeFilter;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.ComponentScan.Filter;

@SpringBatchTest
@SpringBootTest(classes= {DBTestConfig.class})
@ComponentScan(excludeFilters = {
		@Filter(type = FilterType.CUSTOM, classes = TypeExcludeFilter.class),
		@Filter(type = FilterType.CUSTOM, classes = AutoConfigurationExcludeFilter.class),
		@Filter(type = FilterType.REGEX, pattern = "gov.acwi.nwqmc.etl.Application")})
public abstract class BaseStepIT extends BaseIT {

	@Autowired
	protected JobLauncherTestUtils jobLauncherTestUtils;

	@Autowired
	protected JobRepositoryTestUtils jobRepositoryTestUtils;

	protected JobParameters getJobParameters() {
		return new JobParametersBuilder()
			.addJobParameters(jobLauncherTestUtils.getUniqueJobParameters())
			.addString("datasource", "STORET")
			.toJobParameters();
	}

}
