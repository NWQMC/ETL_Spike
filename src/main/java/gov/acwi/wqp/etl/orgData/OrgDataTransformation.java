package gov.acwi.wqp.etl.orgData;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OrgDataTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropOrgDataIndexes")
	private Tasklet dropOrgDataIndexes;

	@Autowired
	@Qualifier("truncateOrgData")
	private Tasklet truncateOrgData;

	@Autowired
	@Qualifier("transformOrgDataWqx")
	private Tasklet transformOrgDataWqx;

	@Autowired
	@Qualifier("transformOrgDataStoretw")
	private Tasklet transformOrgDataStoretw;

	@Autowired
	@Qualifier("buildOrgDataIndexes")
	private Tasklet buildOrgDataIndexes;

	@Bean
	public Flow orgDataFlow() {
		return new FlowBuilder<SimpleFlow>("orgDataFlow")
				.start(dropOrgDataIndexesStep())
				.next(truncateOrgDataStep())
				.next(transformOrgDataWqxStep())
				.next(transformOrgDataStoretwStep())
				.next(buildOrgDataIndexesStep())
				.build();
	}

	@Bean
	public Step dropOrgDataIndexesStep() {
		return stepBuilderFactory.get("dropOrgDataIndexesStep")
				.tasklet(dropOrgDataIndexes)
				.build();
	}

	@Bean
	public Step truncateOrgDataStep() {
		return stepBuilderFactory.get("truncateOrgDataStep")
				.tasklet(truncateOrgData)
				.build();
	}

	@Bean
	public Step transformOrgDataWqxStep() {
		return stepBuilderFactory.get("transformOrgDataWqxStep")
				.tasklet(transformOrgDataWqx)
				.build();
	}

	@Bean
	public Step transformOrgDataStoretwStep() {
		return stepBuilderFactory.get("transformOrgDataStoretwStep")
				.tasklet(transformOrgDataStoretw)
				.build();
	}

	@Bean
	public Step buildOrgDataIndexesStep() {
		return stepBuilderFactory.get("buildOrgDataIndexesStep")
				.tasklet(buildOrgDataIndexes)
				.build();
	}
}
