package gov.acwi.wqp.etl.biologicalHabitatMetric;

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
public class BiologicalHabitatMetricTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropBiologicalHabitatMetricIndexes")
	private Tasklet dropBiologicalHabitatMetricIndexes;

	@Autowired
	@Qualifier("truncateBiologicalHabitatMetric")
	private Tasklet truncateBiologicalHabitatMetric;

	@Autowired
	@Qualifier("transformBiologicalHabitatMetricWqx")
	private Tasklet transformBiologicalHabitatMetricWqx;

	@Autowired
	@Qualifier("buildBiologicalHabitatMetricIndexes")
	private Tasklet buildBiologicalHabitatMetricIndexes;

	@Bean
	public Flow biologicalHabitatMetricFlow() {
		return new FlowBuilder<SimpleFlow>("biologicalHabitatMetricFlow")
				.start(dropBiologicalHabitatMetricIndexesStep())
				.next(truncateBiologicalHabitatMetricStep())
				.next(transformBiologicalHabitatMetricWqxStep())
				.next(buildBiologicalHabitatMetricIndexesStep())
				.build();
	}

	@Bean
	public Step dropBiologicalHabitatMetricIndexesStep() {
		return stepBuilderFactory.get("dropBiologicalHabitatMetricIndexesStep")
				.tasklet(dropBiologicalHabitatMetricIndexes)
				.build();
	}

	@Bean
	public Step truncateBiologicalHabitatMetricStep() {
		return stepBuilderFactory.get("truncateBiologicalHabitatMetricStep")
				.tasklet(truncateBiologicalHabitatMetric)
				.build();
	}

	@Bean
	public Step transformBiologicalHabitatMetricWqxStep() {
		return stepBuilderFactory.get("transformBiologicalHabitatMetricWqxStep")
				.tasklet(transformBiologicalHabitatMetricWqx)
				.build();
	}

	@Bean
	public Step buildBiologicalHabitatMetricIndexesStep() {
		return stepBuilderFactory.get("buildBiologicalHabitatMetricIndexesStep")
				.tasklet(buildBiologicalHabitatMetricIndexes)
				.build();
	}
}
