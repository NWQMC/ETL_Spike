package gov.acwi.nwqmc.etl.summary;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class CreateSummaries {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("createSummaryTables")
	public CreateSummaryTables createSummaryTables;

	@Autowired
	@Qualifier("createSummaryIndexes")
	public CreateSummaryIndexes createSummaryIndexes;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("createSummaries")
				.start(createSummaryTables())
				.next(createSummaryIndexes())
				.build();
	}

	public Step createSummaryTables() {
		return stepBuilderFactory.get("createSummaryTables")
				.tasklet(createSummaryTables)
				.build();
	}

	public Step createSummaryIndexes() {
		return stepBuilderFactory.get("createSummaryIndexes")
				.tasklet(createSummaryIndexes)
				.build();
	}
}
