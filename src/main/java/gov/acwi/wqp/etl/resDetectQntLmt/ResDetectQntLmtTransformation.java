package gov.acwi.wqp.etl.resDetectQntLmt;

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
public class ResDetectQntLmtTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropResDetectQntLmtIndexes")
	private Tasklet dropResDetectQntLmtIndexes;

	@Autowired
	@Qualifier("truncateResDetectQntLmt")
	private Tasklet truncateResDetectQntLmt;

	@Autowired
	@Qualifier("transformResDetectQntLmtWqx")
	private Tasklet transformResDetectQntLmtWqx;

	@Autowired
	@Qualifier("transformResDetectQntLmtStoretw")
	private Tasklet transformResDetectQntLmtStoretw;

	@Autowired
	@Qualifier("buildResDetectQntLmtIndexes")
	private Tasklet buildResDetectQntLmtIndexes;

	@Bean
	public Flow resDetectQntLmtFlow() {
		return new FlowBuilder<SimpleFlow>("resDetectQntLmtFlow")
				.start(dropResDetectQntLmtIndexesStep())
				.next(truncateResDetectQntLmtStep())
				.next(transformResDetectQntLmtWqxStep())
				.next(transformResDetectQntLmtStoretwStep())
				.next(buildResDetectQntLmtIndexesStep())
				.build();
	}

	@Bean
	public Step dropResDetectQntLmtIndexesStep() {
		return stepBuilderFactory.get("dropResDetectQntLmtIndexesStep")
				.tasklet(dropResDetectQntLmtIndexes)
				.build();
	}

	@Bean
	public Step truncateResDetectQntLmtStep() {
		return stepBuilderFactory.get("truncateResDetectQntLmtStep")
				.tasklet(truncateResDetectQntLmt)
				.build();
	}

	@Bean
	public Step transformResDetectQntLmtWqxStep() {
		return stepBuilderFactory.get("transformResDetectQntLmtWqxStep")
				.tasklet(transformResDetectQntLmtWqx)
				.build();
	}

	@Bean
	public Step transformResDetectQntLmtStoretwStep() {
		return stepBuilderFactory.get("transformResDetectQntLmtStoretwStep")
				.tasklet(transformResDetectQntLmtStoretw)
				.build();
	}

	@Bean
	public Step buildResDetectQntLmtIndexesStep() {
		return stepBuilderFactory.get("buildResDetectQntLmtIndexesStep")
				.tasklet(buildResDetectQntLmtIndexes)
				.build();
	}
}
