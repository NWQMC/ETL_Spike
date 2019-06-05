package gov.acwi.wqp.etl.projectMLWeighting;

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
public class TransformProjectMLWeighting {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;


	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_PROJECT_ML_WEIGHTING_SWAP_TABLE_FLOW)
	private Flow setupProjectMLWeightingSwapTableFlow;

	@Autowired
	@Qualifier("transformProjectMLWeightingWqx")
	private Tasklet transformProjectMLWeightingWqx;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_PROJECT_ML_WEIGHTING_FLOW)
	private Flow afterLoadProjectMLWeightingFlow;

	@Bean
	public Step transformProjectMLWeightingWqxStep() {
		return stepBuilderFactory.get("transformProjectMLWeightingWqxStep")
				.tasklet(transformProjectMLWeightingWqx)
				.build();
	}

	@Bean
	public Flow projectMLWeightingFlow() {
		return new FlowBuilder<SimpleFlow>("projectMLWeightingFlow")
				.start(setupProjectMLWeightingSwapTableFlow)
				//TODO - WQP-1429
//				.next(transformProjectMlWeightingWqxStep())
				.next(afterLoadProjectMLWeightingFlow)
				.build();
	}
}
