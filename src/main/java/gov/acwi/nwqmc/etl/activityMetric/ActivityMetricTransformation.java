package gov.acwi.nwqmc.etl.activityMetric;

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
public class ActivityMetricTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropActivityMetricIndexes")
	public Tasklet dropActivityMetricIndexes;

	@Autowired
	@Qualifier("truncateActivityMetric")
	public Tasklet truncateActivityMetric;

	@Autowired
	@Qualifier("transformActivityMetricWqx")
	public Tasklet transformActivityMetricWqx;

	@Autowired
	@Qualifier("buildActivityMetricIndexes")
	public Tasklet buildActivityMetricIndexes;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("activityMetric")
				.start(dropActivityMetricIndexes())
				.next(truncateActivityMetric())
				.next(transformActivityMetricWqx())
				.next(buildActivityMetricIndexes())
				.build();
	}

	public Step dropActivityMetricIndexes() {
		return stepBuilderFactory.get("dropActivityMetricIndexes")
				.tasklet(dropActivityMetricIndexes)
				.build();
	}

	public Step truncateActivityMetric() {
		return stepBuilderFactory.get("truncateActivityMetric")
				.tasklet(truncateActivityMetric)
				.build();
	}

	public Step transformActivityMetricWqx() {
		return stepBuilderFactory.get("transformActivityMetricWqx")
				.tasklet(transformActivityMetricWqx)
				.build();
	}

	public Step buildActivityMetricIndexes() {
		return stepBuilderFactory.get("buildActivityMetricIndexes")
				.tasklet(buildActivityMetricIndexes)
				.build();
	}
}
