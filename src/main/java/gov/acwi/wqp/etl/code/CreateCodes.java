package gov.acwi.wqp.etl.code;

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
public class CreateCodes {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("createCodeTables")
	private CreateCodeTables createCodeTables;

	@Bean
	public Flow createCodesFlow() {
		return new FlowBuilder<SimpleFlow>("createCodesFlow")
				.start(createCodeTablesStep())
				.build();
	}

	@Bean
	public Step createCodeTablesStep() {
		return stepBuilderFactory.get("createCodeTablesStep")
				.tasklet(createCodeTables)
				.build();
	}
}
