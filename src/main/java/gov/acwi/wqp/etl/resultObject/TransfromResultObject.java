package gov.acwi.wqp.etl.resultObject;

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
public class TransfromResultObject {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_RESULT_OBJECT_SWAP_TABLE_FLOW)
	private Flow setupResultObjectSwapTableFlow;

	@Autowired
	@Qualifier("transformResultObjectWqx")
	private Tasklet transformResultObjectWqx;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_RESULT_OBJECT_FLOW)
	private Flow afterLoadResultObjectFlow;

	@Bean
	public Step transformResultObjectWqxStep() {
		return stepBuilderFactory.get("transformResultObjectWqxStep")
				.tasklet(transformResultObjectWqx)
				.build();
	}

	@Bean
	public Flow resultObjectFlow() {
		return new FlowBuilder<SimpleFlow>("resultObjectFlow")
				.start(setupResultObjectSwapTableFlow)
				.next(transformResultObjectWqxStep())
				.next(afterLoadResultObjectFlow)
				.build();
	}
}
