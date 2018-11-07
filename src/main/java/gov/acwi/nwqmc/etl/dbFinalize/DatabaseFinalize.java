package gov.acwi.nwqmc.etl.dbFinalize;

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
public class DatabaseFinalize {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("addRi")
	private Tasklet addRi;

	@Autowired
	@Qualifier("analyze")
	private Tasklet analyze;

	@Autowired
	@Qualifier("validate")
	private Tasklet validate;

	@Autowired
	@Qualifier("install")
	private Tasklet install;

	@Autowired
	@Qualifier("finalize")
	private Tasklet finalize;

	@Bean
	public Flow databaseFinalizeFlow() {
		return new FlowBuilder<SimpleFlow>("databaseFinalizeFlow")
				.start(addRiStep())
				.next(analyzeStep())
//TODO				.next(validateStep())
				.next(installStep())
				.next(finalizeStep())
				.build();
	}

	@Bean
	public Step addRiStep() {
		return stepBuilderFactory.get("addRiStep")
				.tasklet(addRi)
				.build();
	}

	@Bean
	public Step analyzeStep() {
		return stepBuilderFactory.get("analyzeStep")
				.tasklet(analyze)
				.build();
	}

	@Bean
	public Step validateStep() {
		return stepBuilderFactory.get("validateStep")
				.tasklet(validate)
				.build();
	}

	@Bean
	public Step installStep() {
		return stepBuilderFactory.get("installStep")
				.tasklet(install)
				.build();
	}

	@Bean
	public Step finalizeStep() {
		return stepBuilderFactory.get("finalizeStep")
				.tasklet(finalize)
				.build();
	}
}
