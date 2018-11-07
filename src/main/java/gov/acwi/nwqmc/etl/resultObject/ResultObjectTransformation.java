package gov.acwi.nwqmc.etl.resultObject;

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
public class ResultObjectTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropResultObjectIndexes")
	private Tasklet dropResultObjectIndexes;

	@Autowired
	@Qualifier("truncateResultObject")
	private Tasklet truncateResultObject;

	@Autowired
	@Qualifier("transformResultObjectWqx")
	private Tasklet transformResultObjectWqx;

	@Autowired
	@Qualifier("buildResultObjectIndexes")
	private Tasklet buildResultObjectIndexes;

	@Bean
	public Flow resultObjectFlow() {
		return new FlowBuilder<SimpleFlow>("resultObjectFlow")
				.start(dropResultObjectIndexesStep())
				.next(truncateResultObjectStep())
				.next(transformResultObjectWqxStep())
				.next(buildResultObjectIndexesStep())
				.build();
	}

	@Bean
	public Step dropResultObjectIndexesStep() {
		return stepBuilderFactory.get("dropResultObjectIndexesStep")
				.tasklet(dropResultObjectIndexes)
				.build();
	}

	@Bean
	public Step truncateResultObjectStep() {
		return stepBuilderFactory.get("truncateResultObjectStep")
				.tasklet(truncateResultObject)
				.build();
	}

	@Bean
	public Step transformResultObjectWqxStep() {
		return stepBuilderFactory.get("transformResultObjectWqxStep")
				.tasklet(transformResultObjectWqx)
				.build();
	}

	@Bean
	public Step buildResultObjectIndexesStep() {
		return stepBuilderFactory.get("buildResultObjectIndexesStep")
				.tasklet(buildResultObjectIndexes)
				.build();
	}
}
