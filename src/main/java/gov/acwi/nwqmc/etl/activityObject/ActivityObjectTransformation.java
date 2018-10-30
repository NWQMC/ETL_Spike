package gov.acwi.nwqmc.etl.activityObject;

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
public class ActivityObjectTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropActivityObjectIndexes")
	public Tasklet dropActivityObjectIndexes;

	@Autowired
	@Qualifier("truncateActivityObject")
	public Tasklet truncateActivityObject;

	@Autowired
	@Qualifier("transformActivityObjectWqx")
	public Tasklet transformActivityObjectWqx;

	@Autowired
	@Qualifier("buildActivityObjectIndexes")
	public Tasklet buildActivityObjectIndexes;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("activityObject")
				.start(dropActivityObjectIndexes())
				.next(truncateActivityObject())
				.next(transformActivityObjectWqx())
				.next(buildActivityObjectIndexes())
				.build();
	}

	public Step dropActivityObjectIndexes() {
		return stepBuilderFactory.get("dropActivityObjectIndexes")
				.tasklet(dropActivityObjectIndexes)
				.build();
	}

	public Step truncateActivityObject() {
		return stepBuilderFactory.get("truncateActivityObject")
				.tasklet(truncateActivityObject)
				.build();
	}

	public Step transformActivityObjectWqx() {
		return stepBuilderFactory.get("transformActivityObjectWqx")
				.tasklet(transformActivityObjectWqx)
				.build();
	}

	public Step buildActivityObjectIndexes() {
		return stepBuilderFactory.get("buildActivityObjectIndexes")
				.tasklet(buildActivityObjectIndexes)
				.build();
	}
}
