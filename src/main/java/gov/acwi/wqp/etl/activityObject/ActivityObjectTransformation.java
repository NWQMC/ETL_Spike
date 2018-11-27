package gov.acwi.wqp.etl.activityObject;

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
public class ActivityObjectTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropActivityObjectIndexes")
	private Tasklet dropActivityObjectIndexes;

	@Autowired
	@Qualifier("truncateActivityObject")
	private Tasklet truncateActivityObject;

	@Autowired
	@Qualifier("transformActivityObjectWqx")
	private Tasklet transformActivityObjectWqx;

	@Autowired
	@Qualifier("buildActivityObjectIndexes")
	private Tasklet buildActivityObjectIndexes;

	@Bean
	public Flow activityObjectFlow() {
		return new FlowBuilder<SimpleFlow>("activityObjectFlow")
				.start(dropActivityObjectIndexesStep())
				.next(truncateActivityObjectStep())
				.next(transformActivityObjectWqxStep())
				.next(buildActivityObjectIndexesStep())
				.build();
	}

	@Bean
	public Step dropActivityObjectIndexesStep() {
		return stepBuilderFactory.get("dropActivityObjectIndexesStep")
				.tasklet(dropActivityObjectIndexes)
				.build();
	}

	@Bean
	public Step truncateActivityObjectStep() {
		return stepBuilderFactory.get("truncateActivityObjectStep")
				.tasklet(truncateActivityObject)
				.build();
	}

	@Bean
	public Step transformActivityObjectWqxStep() {
		return stepBuilderFactory.get("transformActivityObjectWqxStep")
				.tasklet(transformActivityObjectWqx)
				.build();
	}

	@Bean
	public Step buildActivityObjectIndexesStep() {
		return stepBuilderFactory.get("buildActivityObjectIndexesStep")
				.tasklet(buildActivityObjectIndexes)
				.build();
	}
}
