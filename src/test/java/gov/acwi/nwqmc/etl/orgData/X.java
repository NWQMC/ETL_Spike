package gov.acwi.nwqmc.etl.orgData;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
@EnableAutoConfiguration
public class X {
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	@Qualifier("transformOrgDataWqxStep")
	private Step transformOrgDataWqxStep;
	@Bean
	public Job wqxEtl() {
		return jobBuilderFactory.get("test")
				.start(transformOrgDataWqxStep)
				.build();
	}

}
