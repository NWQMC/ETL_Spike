package gov.acwi.nwqmc.etl.monitoringLocationObject;

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
public class MonitoringLocationObjectTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropMonitoringLocationObjectIndexes")
	private Tasklet dropMonitoringLocationObjectIndexes;

	@Autowired
	@Qualifier("truncateMonitoringLocationObject")
	private Tasklet truncateMonitoringLocationObject;

	@Autowired
	@Qualifier("transformMonitoringLocationObjectWqx")
	private Tasklet transformMonitoringLocationObjectWqx;

	@Autowired
	@Qualifier("buildMonitoringLocationObjectIndexes")
	private Tasklet buildMonitoringLocationObjectIndexes;

	@Bean
	public Flow monitoringLocationObjectFlow() {
		return new FlowBuilder<SimpleFlow>("monitoringLocationObjectFlow")
				.start(dropMonitoringLocationObjectIndexesStep())
				.next(truncateMonitoringLocationObjectStep())
				.next(transformMonitoringLocationObjectWqxStep())
				.next(buildMonitoringLocationObjectIndexesStep())
				.build();
	}

	@Bean
	public Step dropMonitoringLocationObjectIndexesStep() {
		return stepBuilderFactory.get("dropMonitoringLocationObjectIndexesStep")
				.tasklet(dropMonitoringLocationObjectIndexes)
				.build();
	}

	@Bean
	public Step truncateMonitoringLocationObjectStep() {
		return stepBuilderFactory.get("truncateMonitoringLocationObjectStep")
				.tasklet(truncateMonitoringLocationObject)
				.build();
	}

	@Bean
	public Step transformMonitoringLocationObjectWqxStep() {
		return stepBuilderFactory.get("transformMonitoringLocationObjectWqxStep")
				.tasklet(transformMonitoringLocationObjectWqx)
				.build();
	}

	@Bean
	public Step buildMonitoringLocationObjectIndexesStep() {
		return stepBuilderFactory.get("buildMonitoringLocationObjectIndexesStep")
				.tasklet(buildMonitoringLocationObjectIndexes)
				.build();
	}
}
