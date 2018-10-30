package gov.acwi.nwqmc.etl.resDetectQntLmt;

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
public class ResDetectQntLmtTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropResDetectQntLmtIndexes")
	public Tasklet dropResDetectQntLmtIndexes;

	@Autowired
	@Qualifier("truncateResDetectQntLmt")
	public Tasklet truncateResDetectQntLmt;

	@Autowired
	@Qualifier("transformResDetectQntLmtWqx")
	public Tasklet transformResDetectQntLmtWqx;

	@Autowired
	@Qualifier("transformResDetectQntLmtStoretw")
	public Tasklet transformResDetectQntLmtStoretw;

	@Autowired
	@Qualifier("buildResDetectQntLmtIndexes")
	public Tasklet buildResDetectQntLmtIndexes;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("resDetectQntLmt")
				.start(dropResDetectQntLmtIndexes())
				.next(truncateResDetectQntLmt())
				.next(transformResDetectQntLmtWqx())
				.next(transformResDetectQntLmtStoretw())
				.next(buildResDetectQntLmtIndexes())
				.build();
	}

	public Step dropResDetectQntLmtIndexes() {
		return stepBuilderFactory.get("dropResDetectQntLmtIndexes")
				.tasklet(dropResDetectQntLmtIndexes)
				.build();
	}

	public Step truncateResDetectQntLmt() {
		return stepBuilderFactory.get("truncateResDetectQntLmt")
				.tasklet(truncateResDetectQntLmt)
				.build();
	}

	public Step transformResDetectQntLmtWqx() {
		return stepBuilderFactory.get("transformResDetectQntLmtWqx")
				.tasklet(transformResDetectQntLmtWqx)
				.build();
	}

	public Step transformResDetectQntLmtStoretw() {
		return stepBuilderFactory.get("transformResDetectQntLmtStoretw")
				.tasklet(transformResDetectQntLmtStoretw)
				.build();
	}

	public Step buildResDetectQntLmtIndexes() {
		return stepBuilderFactory.get("buildResDetectQntLmtIndexes")
				.tasklet(buildResDetectQntLmtIndexes)
				.build();
	}
}
