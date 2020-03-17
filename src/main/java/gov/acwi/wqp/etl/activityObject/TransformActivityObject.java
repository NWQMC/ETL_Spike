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

import gov.acwi.wqp.etl.EtlConstantUtils;

@Configuration
public class TransformActivityObject {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_ACTIVITY_OBJECT_SWAP_TABLE_FLOW)
	private Flow setupActivityObjectSwapTableFlow;

	@Autowired
	@Qualifier("transformActivityObjectWqx")
	private Tasklet transformActivityObjectWqx;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_ACTIVITY_OBJECT_FLOW)
	private Flow afterLoadActivityObjectFlow;

	@Bean
	public Step transformActivityObjectWqxStep() {
		return stepBuilderFactory.get("transformActivityObjectWqxStep")
				.tasklet(transformActivityObjectWqx)
				.build();
	}

	@Bean
	public Flow activityObjectFlow() {
		return new FlowBuilder<SimpleFlow>("activityObjectFlow")
				.start(setupActivityObjectSwapTableFlow)
				.next(transformActivityObjectWqxStep())
				.next(afterLoadActivityObjectFlow)
				.build();
	}
}
