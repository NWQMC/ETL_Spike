package gov.acwi.nwqmc.etl.monitoringLocationObject;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class MonitoringLocationObjectTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropMonitoringLocationObjectIndexes")
	public Tasklet dropMonitoringLocationObjectIndexes;

	@Autowired
	@Qualifier("truncateMonitoringLocationObject")
	public Tasklet truncateMonitoringLocationObject;

	@Autowired
	@Qualifier("transformMonitoringLocationObjectWqx")
	public Tasklet transformMonitoringLocationObjectWqx;

	@Autowired
	@Qualifier("buildMonitoringLocationObjectIndexes")
	public Tasklet buildMonitoringLocationObjectIndexes;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("monitoringLocationObject")
				.start(dropMonitoringLocationObjectIndexes())
				.next(truncateMonitoringLocationObject())
				.next(transformMonitoringLocationObjectWqx())
				.next(buildMonitoringLocationObjectIndexes())
				.build();
	}

	public Step dropMonitoringLocationObjectIndexes() {
		return stepBuilderFactory.get("dropMonitoringLocationObjectIndexes")
				.tasklet(dropMonitoringLocationObjectIndexes)
				.build();
	}

	public Step truncateMonitoringLocationObject() {
		return stepBuilderFactory.get("truncateMonitoringLocationObject")
				.tasklet(truncateMonitoringLocationObject)
				.build();
	}

	public Step transformMonitoringLocationObjectWqx() {
		return stepBuilderFactory.get("transformMonitoringLocationObjectWqx")
				.tasklet(transformMonitoringLocationObjectWqx)
				.build();
	}

	public Step buildMonitoringLocationObjectIndexes() {
		return stepBuilderFactory.get("buildMonitoringLocationObjectIndexes")
				.tasklet(buildMonitoringLocationObjectIndexes)
				.build();
	}
}
