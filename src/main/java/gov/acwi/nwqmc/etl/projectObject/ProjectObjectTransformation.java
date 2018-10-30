package gov.acwi.nwqmc.etl.projectObject;

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
public class ProjectObjectTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropProjectObjectIndexes")
	public Tasklet dropProjectObjectIndexes;

	@Autowired
	@Qualifier("truncateProjectObject")
	public Tasklet truncateProjectObject;

	@Autowired
	@Qualifier("transformProjectObjectWqx")
	public Tasklet transformProjectObjectWqx;

	@Autowired
	@Qualifier("buildProjectObjectIndexes")
	public Tasklet buildProjectObjectIndexes;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("projectObject")
				.start(dropProjectObjectIndexes())
				.next(truncateProjectObject())
				.next(transformProjectObjectWqx())
				.next(buildProjectObjectIndexes())
				.build();
	}

	public Step dropProjectObjectIndexes() {
		return stepBuilderFactory.get("dropProjectObjectIndexes")
				.tasklet(dropProjectObjectIndexes)
				.build();
	}

	public Step truncateProjectObject() {
		return stepBuilderFactory.get("truncateProjectObject")
				.tasklet(truncateProjectObject)
				.build();
	}

	public Step transformProjectObjectWqx() {
		return stepBuilderFactory.get("transformProjectObjectWqx")
				.tasklet(transformProjectObjectWqx)
				.build();
	}

	public Step buildProjectObjectIndexes() {
		return stepBuilderFactory.get("buildProjectObjectIndexes")
				.tasklet(buildProjectObjectIndexes)
				.build();
	}
}
