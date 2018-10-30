package gov.acwi.nwqmc.etl.biologicalHabitatMetric;

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
public class BiologicalHabitatMetricTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropBiologicalHabitatMetricIndexes")
	public Tasklet dropBiologicalHabitatMetricIndexes;

	@Autowired
	@Qualifier("truncateBiologicalHabitatMetric")
	public Tasklet truncateBiologicalHabitatMetric;

	@Autowired
	@Qualifier("transformBiologicalHabitatMetricWqx")
	public Tasklet transformBiologicalHabitatMetricWqx;

	@Autowired
	@Qualifier("buildBiologicalHabitatMetricIndexes")
	public Tasklet buildBiologicalHabitatMetricIndexes;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("biologicalHabitatMetricWqx")
				.start(dropBiologicalHabitatMetricIndexes())
				.next(truncateBiologicalHabitatMetric())
				.next(transformBiologicalHabitatMetricWqx())
				.next(buildBiologicalHabitatMetricIndexes())
				.build();
	}

	public Step dropBiologicalHabitatMetricIndexes() {
		return stepBuilderFactory.get("dropBiologicalHabitatMetricIndexes")
				.tasklet(dropBiologicalHabitatMetricIndexes)
				.build();
	}

	public Step truncateBiologicalHabitatMetric() {
		return stepBuilderFactory.get("truncateBiologicalHabitatMetric")
				.tasklet(truncateBiologicalHabitatMetric)
				.build();
	}

	public Step transformBiologicalHabitatMetricWqx() {
		return stepBuilderFactory.get("transformBiologicalHabitatMetricWqx")
				.tasklet(transformBiologicalHabitatMetricWqx)
				.build();
	}

	public Step buildBiologicalHabitatMetricIndexes() {
		return stepBuilderFactory.get("buildBiologicalHabitatMetricIndexes")
				.tasklet(buildBiologicalHabitatMetricIndexes)
				.build();
	}
}
