package gov.acwi.nwqmc.etl.projectMlWeighting;

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
public class ProjectMlWeightingTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropProjectMlWeightingIndexes")
	public Tasklet dropProjectMlWeightingIndexes;

	@Autowired
	@Qualifier("truncateProjectMlWeighting")
	public Tasklet truncateProjectMlWeighting;

	@Autowired
	@Qualifier("transformProjectMlWeightingWqx")
	public Tasklet transformProjectMlWeightingWqx;

	@Autowired
	@Qualifier("buildProjectMlWeightingIndexes")
	public Tasklet buildProjectMlWeightingIndexes;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("projectMlWeighting")
				.start(dropProjectMlWeightingIndexes())
				.next(truncateProjectMlWeighting())
				.next(transformProjectMlWeightingWqx())
				.next(buildProjectMlWeightingIndexes())
				.build();
	}

	public Step dropProjectMlWeightingIndexes() {
		return stepBuilderFactory.get("dropProjectMlWeightingIndexes")
				.tasklet(dropProjectMlWeightingIndexes)
				.build();
	}

	public Step truncateProjectMlWeighting() {
		return stepBuilderFactory.get("truncateProjectMlWeighting")
				.tasklet(truncateProjectMlWeighting)
				.build();
	}

	public Step transformProjectMlWeightingWqx() {
		return stepBuilderFactory.get("transformProjectMlWeightingWqx")
				.tasklet(transformProjectMlWeightingWqx)
				.build();
	}

	public Step buildProjectMlWeightingIndexes() {
		return stepBuilderFactory.get("buildProjectMlWeightingIndexes")
				.tasklet(buildProjectMlWeightingIndexes)
				.build();
	}
}
