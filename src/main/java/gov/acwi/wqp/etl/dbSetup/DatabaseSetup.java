package gov.acwi.wqp.etl.dbSetup;

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
public class DatabaseSetup {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropRi")
	private Tasklet dropRi;

	@Bean
	public Flow databaseSetupFlow() {
		return new FlowBuilder<SimpleFlow>("databaseSetupFlow")
				.start(dropRiStep())
				.build();
	}

	@Bean
	public Step dropRiStep() {
		return stepBuilderFactory.get("dropRiStep")
				.tasklet(dropRi)
				.build();
	}
}
