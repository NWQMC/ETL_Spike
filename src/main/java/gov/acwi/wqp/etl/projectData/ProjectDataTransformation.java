package gov.acwi.wqp.etl.projectData;

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
public class ProjectDataTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropProjectDataIndexes")
	private Tasklet dropProjectDataIndexes;

	@Autowired
	@Qualifier("truncateProjectData")
	private Tasklet truncateProjectData;

	@Autowired
	@Qualifier("transformProjectDataWqx")
	private Tasklet transformProjectDataWqx;

	@Autowired
	@Qualifier("transformProjectDataStoretw")
	private Tasklet transformProjectDataStoretw;

	@Autowired
	@Qualifier("buildProjectDataIndexes")
	private Tasklet buildProjectDataIndexes;

	@Bean
	public Flow projectDataFlow() {
		return new FlowBuilder<SimpleFlow>("projectDataFlow")
				.start(dropProjectDataIndexesStep())
				.next(truncateProjectDataStep())
				.next(transformProjectDataWqxStep())
				.next(transformProjectDataStoretwStep())
				.next(buildProjectDataIndexesStep())
				.build();
	}

	@Bean
	public Step dropProjectDataIndexesStep() {
		return stepBuilderFactory.get("dropProjectDataIndexesStep")
				.tasklet(dropProjectDataIndexes)
				.build();
	}

	@Bean
	public Step truncateProjectDataStep() {
		return stepBuilderFactory.get("truncateProjectDataStep")
				.tasklet(truncateProjectData)
				.build();
	}

	@Bean
	public Step transformProjectDataWqxStep() {
		return stepBuilderFactory.get("transformProjectDataWqxStep")
				.tasklet(transformProjectDataWqx)
				.build();
	}

	@Bean
	public Step transformProjectDataStoretwStep() {
		return stepBuilderFactory.get("transformProjectDataStoretwStep")
				.tasklet(transformProjectDataStoretw)
				.build();
	}

	@Bean
	public Step buildProjectDataIndexesStep() {
		return stepBuilderFactory.get("buildProjectDataIndexesStep")
				.tasklet(buildProjectDataIndexes)
				.build();
	}
}
