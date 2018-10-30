package gov.acwi.nwqmc.etl.resultObject;

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
public class ResultObjectTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropResultObjectIndexes")
	public Tasklet dropResultObjectIndexes;

	@Autowired
	@Qualifier("truncateResultObject")
	public Tasklet truncateResultObject;

	@Autowired
	@Qualifier("transformResultObjectWqx")
	public Tasklet transformResultObjectWqx;

	@Autowired
	@Qualifier("buildResultObjectIndexes")
	public Tasklet buildResultObjectIndexes;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("resultObject")
				.start(dropResultObjectIndexes())
				.next(truncateResultObject())
				.next(transformResultObjectWqx())
				.next(buildResultObjectIndexes())
				.build();
	}

	public Step dropResultObjectIndexes() {
		return stepBuilderFactory.get("dropResultObjectIndexes")
				.tasklet(dropResultObjectIndexes)
				.build();
	}

	public Step truncateResultObject() {
		return stepBuilderFactory.get("truncateResultObject")
				.tasklet(truncateResultObject)
				.build();
	}

	public Step transformResultObjectWqx() {
		return stepBuilderFactory.get("transformResultObjectWqx")
				.tasklet(transformResultObjectWqx)
				.build();
	}

	public Step buildResultObjectIndexes() {
		return stepBuilderFactory.get("buildResultObjectIndexes")
				.tasklet(buildResultObjectIndexes)
				.build();
	}
}
