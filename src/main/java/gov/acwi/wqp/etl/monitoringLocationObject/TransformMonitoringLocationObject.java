package gov.acwi.wqp.etl.monitoringLocationObject;

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
public class TransformMonitoringLocationObject {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_MONITORING_LOCATION_OBJECT_SWAP_TABLE_FLOW)
	private Flow setupMonitoringLocationObjectSwapTableFlow;

	@Autowired
	@Qualifier("transformMonitoringLocationObjectWqx")
	private Tasklet transformMonitoringLocationObjectWqx;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_MONITORING_LOCATION_OBJECT_FLOW)
	private Flow afterLoadMonitoringLocationObjectFlow;

	public Step transformMonitoringLocationObjectWqxStep() {
		return stepBuilderFactory.get("transformMonitoringLocationObjectWqxStep")
				.tasklet(transformMonitoringLocationObjectWqx)
				.build();
	}

	@Bean
	public Flow monitoringLocationObjectFlow() {
		return new FlowBuilder<SimpleFlow>("monitoringLocationObjectFlow")
				.start(setupMonitoringLocationObjectSwapTableFlow)
				.next(transformMonitoringLocationObjectWqxStep())
				.next(afterLoadMonitoringLocationObjectFlow)
				.build();
	}
}
