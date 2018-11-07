package gov.acwi.nwqmc.etl.projectObject;

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
public class ProjectObjectTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropProjectObjectIndexes")
	private Tasklet dropProjectObjectIndexes;

	@Autowired
	@Qualifier("truncateProjectObject")
	private Tasklet truncateProjectObject;

	@Autowired
	@Qualifier("transformProjectObjectWqx")
	private Tasklet transformProjectObjectWqx;

	@Autowired
	@Qualifier("buildProjectObjectIndexes")
	private Tasklet buildProjectObjectIndexes;

	@Bean
	public Flow projectObjectFlow() {
		return new FlowBuilder<SimpleFlow>("projectObjectFlow")
				.start(dropProjectObjectIndexesStep())
				.next(truncateProjectObjectStep())
				.next(transformProjectObjectWqxStep())
				.next(buildProjectObjectIndexesStep())
				.build();
	}

	@Bean
	public Step dropProjectObjectIndexesStep() {
		return stepBuilderFactory.get("dropProjectObjectIndexesStep")
				.tasklet(dropProjectObjectIndexes)
				.build();
	}

	@Bean
	public Step truncateProjectObjectStep() {
		return stepBuilderFactory.get("truncateProjectObjectStep")
				.tasklet(truncateProjectObject)
				.build();
	}

	@Bean
	public Step transformProjectObjectWqxStep() {
		return stepBuilderFactory.get("transformProjectObjectWqxStep")
				.tasklet(transformProjectObjectWqx)
				.build();
	}

	@Bean
	public Step buildProjectObjectIndexesStep() {
		return stepBuilderFactory.get("buildProjectObjectIndexesStep")
				.tasklet(buildProjectObjectIndexes)
				.build();
	}
}
