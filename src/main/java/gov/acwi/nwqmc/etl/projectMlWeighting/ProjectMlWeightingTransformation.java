package gov.acwi.nwqmc.etl.projectMlWeighting;

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
public class ProjectMlWeightingTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropProjectMlWeightingIndexes")
	private Tasklet dropProjectMlWeightingIndexes;

	@Autowired
	@Qualifier("truncateProjectMlWeighting")
	private Tasklet truncateProjectMlWeighting;

	@Autowired
	@Qualifier("transformProjectMlWeightingWqx")
	private Tasklet transformProjectMlWeightingWqx;

	@Autowired
	@Qualifier("buildProjectMlWeightingIndexes")
	private Tasklet buildProjectMlWeightingIndexes;

	@Bean
	public Flow projectMlWeightingFlow() {
		return new FlowBuilder<SimpleFlow>("projectMlWeightingFlow")
				.start(dropProjectMlWeightingIndexesStep())
				.next(truncateProjectMlWeightingStep())
				.next(transformProjectMlWeightingWqxStep())
				.next(buildProjectMlWeightingIndexesStep())
				.build();
	}

	@Bean
	public Step dropProjectMlWeightingIndexesStep() {
		return stepBuilderFactory.get("dropProjectMlWeightingIndexesStep")
				.tasklet(dropProjectMlWeightingIndexes)
				.build();
	}

	@Bean
	public Step truncateProjectMlWeightingStep() {
		return stepBuilderFactory.get("truncateProjectMlWeightingStep")
				.tasklet(truncateProjectMlWeighting)
				.build();
	}

	@Bean
	public Step transformProjectMlWeightingWqxStep() {
		return stepBuilderFactory.get("transformProjectMlWeightingWqxStep")
				.tasklet(transformProjectMlWeightingWqx)
				.build();
	}

	@Bean
	public Step buildProjectMlWeightingIndexesStep() {
		return stepBuilderFactory.get("buildProjectMlWeightingIndexesStep")
				.tasklet(buildProjectMlWeightingIndexes)
				.build();
	}
}
