package gov.acwi.wqp.etl.orgData;

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
public class TransformOrgData {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_ORG_DATA_SWAP_TABLE_FLOW)
	private Flow setupOrgDataSwapTableFlow;

	@Autowired
	@Qualifier("transformOrgDataWqx")
	private Tasklet transformOrgDataWqx;

	@Autowired
	@Qualifier("transformOrgDataStoretw")
	private Tasklet transformOrgDataStoretw;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_ORG_DATA_FLOW)
	private Flow afterLoadOrgDataFlow;

	@Bean
	public Step transformOrgDataWqxStep() {
		return stepBuilderFactory.get("transformOrgDataWqxStep")
				.tasklet(transformOrgDataWqx)
				.build();
	}

	@Bean
	public Step transformOrgDataStoretwStep() {
		return stepBuilderFactory.get("transformOrgDataStoretwStep")
				.tasklet(transformOrgDataStoretw)
				.build();
	}

	@Bean
	public Flow orgDataFlow() {
		return new FlowBuilder<SimpleFlow>("orgDataFlow")
				.start(setupOrgDataSwapTableFlow)
				//TODO - WQP-1415
//				.next(transformOrgDataWqxStep())
//				.next(transformOrgDataStoretwStep())
				.next(afterLoadOrgDataFlow)
				.build();
	}
}
