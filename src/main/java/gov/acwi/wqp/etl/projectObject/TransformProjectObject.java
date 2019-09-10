package gov.acwi.wqp.etl.projectObject;

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
public class TransformProjectObject {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_PROJECT_OBJECT_SWAP_TABLE_FLOW)
	private Flow setupProjectObjectSwapTableFlow;

	@Autowired
	@Qualifier("transformProjectObjectWqx")
	private Tasklet transformProjectObjectWqx;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_PROJECT_OBJECT_FLOW)
	private Flow afterLoadProjectObjectFlow;

	@Bean
	public Step transformProjectObjectWqxStep() {
		return stepBuilderFactory.get("transformProjectObjectWqxStep")
				.tasklet(transformProjectObjectWqx)
				.build();
	}

	@Bean
	public Flow projectObjectFlow() {
		return new FlowBuilder<SimpleFlow>("projectObjectFlow")
				.start(setupProjectObjectSwapTableFlow)
				.next(transformProjectObjectWqxStep())
				.next(afterLoadProjectObjectFlow)
				.build();
	}
}
