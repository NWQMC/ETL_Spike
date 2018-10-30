package gov.acwi.nwqmc.etl.code;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class CreateCodes {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("createCodeTables")
	public CreateCodeTables createCodeTables;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("createCodes")
				.start(createCodeTables())
				.build();
	}

	public Step createCodeTables() {
		return stepBuilderFactory.get("createCodeTables")
				.tasklet(createCodeTables)
				.build();
	}
}
