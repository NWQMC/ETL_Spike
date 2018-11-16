package gov.acwi.wqp.etl.summary;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CreateSummaries {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("createSummaryTables")
	private CreateSummaryTables createSummaryTables;

	@Autowired
	@Qualifier("createSummaryIndexes")
	private CreateSummaryIndexes createSummaryIndexes;

	@Bean
	public Flow createSummariesFlow() {
		return new FlowBuilder<SimpleFlow>("createSummariesFlow")
				.start(createSummaryTablesStep())
				.next(createSummaryIndexesStep())
				.build();
	}

	@Bean
	public Step createSummaryTablesStep() {
		return stepBuilderFactory.get("createSummaryTablesStep")
				.tasklet(createSummaryTables)
				.build();
	}

	@Bean
	public Step createSummaryIndexesStep() {
		return stepBuilderFactory.get("createSummaryIndexesStep")
				.tasklet(createSummaryIndexes)
				.build();
	}
}
