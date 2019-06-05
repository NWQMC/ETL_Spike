package gov.acwi.wqp.etl.projectData;

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

import gov.acwi.wqp.etl.EtlConstantUtils;

@Configuration
public class TransformProjectData {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_PROJECT_DATA_SWAP_TABLE_FLOW)
	private Flow setupProjectDataSwapTableFlow;

	@Autowired
	@Qualifier("transformProjectDataWqx")
	private Tasklet transformProjectDataWqx;

	@Autowired
	@Qualifier("transformProjectDataStoretw")
	private Tasklet transformProjectDataStoretw;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_PROJECT_DATA_FLOW)
	private Flow afterLoadProjectDataFlow;

	@Bean
	public Step transformProjectDataWqxStep() {
		return stepBuilderFactory.get("transformProjectDataWqxStep")
				.tasklet(transformProjectDataWqx)
				.build();
	}

	@Bean
	public Step transformProjectDataStoretwStep() {
		return stepBuilderFactory.get("transformProjectDataStoretwStep")
				.tasklet(transformProjectDataStoretw)
				.build();
	}

	@Bean
	public Flow projectDataFlow() {
		return new FlowBuilder<SimpleFlow>("projectDataFlow")
				.start(setupProjectDataSwapTableFlow)
				//TODO - WQP-1416
//				.next(transformProjectDataWqxStep())
//				.next(transformProjectDataStoretwStep())
				.next(afterLoadProjectDataFlow)
				.build();
	}

}
