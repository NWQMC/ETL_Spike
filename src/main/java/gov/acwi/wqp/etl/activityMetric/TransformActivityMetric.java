package gov.acwi.wqp.etl.activityMetric;

import java.io.IOException;

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
public class TransformActivityMetric {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_ACTIVITY_METRIC_SWAP_TABLE_FLOW)
	private Flow setupActivityMetricSwapTableFlow;

	@Autowired
	@Qualifier("transformActivityMetricWqx")
	private Tasklet transformActivityMetricWqx;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_ACTIVITY_METRIC_FLOW)
	private Flow afterLoadActivityMetricFlow;


	@Bean
	public Step transformActivityMetricWqxStep() {
		return stepBuilderFactory.get("transformActivityMetricWqxStep")
				.tasklet(transformActivityMetricWqx)
				.build();
	}

	@Bean
	public Flow activityMetricFlow() throws IOException {
		return new FlowBuilder<SimpleFlow>("activityMetricFlow")
				.start(setupActivityMetricSwapTableFlow)
				//TODO - WQP-1425
				.next(transformActivityMetricWqxStep())
				.next(afterLoadActivityMetricFlow)
				.build();
	}
}
