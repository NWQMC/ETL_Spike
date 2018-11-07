package gov.acwi.nwqmc.etl.activityMetric;

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
public class ActivityMetricTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropActivityMetricIndexes")
	private Tasklet dropActivityMetricIndexes;

	@Autowired
	@Qualifier("truncateActivityMetric")
	private Tasklet truncateActivityMetric;

	@Autowired
	@Qualifier("transformActivityMetricWqx")
	private Tasklet transformActivityMetricWqx;

	@Autowired
	@Qualifier("buildActivityMetricIndexes")
	private Tasklet buildActivityMetricIndexes;

	@Bean
	public Flow activityMetricFlow() {
		return new FlowBuilder<SimpleFlow>("activityMetricFlow")
				.start(dropActivityMetricIndexesStep())
				.next(truncateActivityMetricStep())
				.next(transformActivityMetricWqxStep())
				.next(buildActivityMetricIndexesStep())
				.build();
	}

	@Bean
	public Step dropActivityMetricIndexesStep() {
		return stepBuilderFactory.get("dropActivityMetricIndexesStep")
				.tasklet(dropActivityMetricIndexes)
				.build();
	}

	@Bean
	public Step truncateActivityMetricStep() {
		return stepBuilderFactory.get("truncateActivityMetricStep")
				.tasklet(truncateActivityMetric)
				.build();
	}

	@Bean
	public Step transformActivityMetricWqxStep() {
		return stepBuilderFactory.get("transformActivityMetricWqxStep")
				.tasklet(transformActivityMetricWqx)
				.build();
	}

	@Bean
	public Step buildActivityMetricIndexesStep() {
		return stepBuilderFactory.get("buildActivityMetricIndexesStep")
				.tasklet(buildActivityMetricIndexes)
				.build();
	}
}
