package gov.acwi.nwqmc.etl.orgData;

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
public class OrgDataTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropOrgDataIndexes")
	public Tasklet dropOrgDataIndexes;

	@Autowired
	@Qualifier("truncateOrgData")
	public Tasklet truncateOrgData;

	@Autowired
	@Qualifier("transformOrgDataWqx")
	public Tasklet transformOrgDataWqx;

	@Autowired
	@Qualifier("transformOrgDataStoretw")
	public Tasklet transformOrgDataStoretw;

	@Autowired
	@Qualifier("buildOrgDataIndexes")
	public Tasklet buildOrgDataIndexes;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("orgData")
				.start(dropOrgDataIndexes())
				.next(truncateOrgData())
				.next(transformOrgDataWqx())
				.next(transformOrgDataStoretw())
				.next(buildOrgDataIndexes())
				.build();
	}

	public Step dropOrgDataIndexes() {
		return stepBuilderFactory.get("dropOrgDataIndexes")
				.tasklet(dropOrgDataIndexes)
				.build();
	}

	public Step truncateOrgData() {
		return stepBuilderFactory.get("truncateOrgData")
				.tasklet(truncateOrgData)
				.build();
	}

	public Step transformOrgDataWqx() {
		return stepBuilderFactory.get("transformOrgDataWqx")
				.tasklet(transformOrgDataWqx)
				.build();
	}

	public Step transformOrgDataStoretw() {
		return stepBuilderFactory.get("transformOrgDataStoretw")
				.tasklet(transformOrgDataStoretw)
				.build();
	}

	public Step buildOrgDataIndexes() {
		return stepBuilderFactory.get("buildOrgDataIndexes")
				.tasklet(buildOrgDataIndexes)
				.build();
	}
}
