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

import gov.acwi.wqp.etl.EtlConstantUtils;

@Configuration
public class TransformBiologicalHabitatMetric {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_BIOLOGICAL_HABITAT_METRIC_SWAP_TABLE_FLOW)
	private Flow setupBiologicalHabitatMetricSwapTableFlow;

	@Autowired
	@Qualifier("transformBiologicalHabitatMetricWqx")
	private Tasklet transformBiologicalHabitatMetricWqx;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_BIOLOGICAL_HABITAT_METRIC_FLOW)
	private Flow afterLoadBiologicalHabitatMetricFlow;

	@Bean
	public Step transformBiologicalHabitatMetricWqxStep() {
		return stepBuilderFactory.get("transformBiologicalHabitatMetricWqxStep")
				.tasklet(transformBiologicalHabitatMetricWqx)
				.build();
	}

	@Bean
	public Flow biologicalHabitatMetricFlow() {
		return new FlowBuilder<SimpleFlow>("biologicalHabitatMetricFlow")
				.start(setupBiologicalHabitatMetricSwapTableFlow)
				//TODO - WQP-1458
//				.next(transformBiologicalHabitatMetricWqxStep())
				.next(afterLoadBiologicalHabitatMetricFlow)
				.build();
	}
}
