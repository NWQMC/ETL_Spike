package gov.acwi.nwqmc.etl.activity;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.stereotype.Component;

@Component
public class ActivityTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("truncateWqxActivityProject")
	public TruncateWqxActivityProject truncateWqxActivityProject;

	@Autowired
	@Qualifier("transformWqxActivityProject")
	public TransformWqxActivityProject transformWqxActivityProject;

	@Autowired
	@Qualifier("truncateWqxActivityConductingOrg")
	public TruncateWqxActivityConductingOrg truncateWqxActivityConductingOrg;

	@Autowired
	@Qualifier("transformWqxActivityConductingOrg")
	public TransformWqxActivityConductingOrg transformWqxActivityConductingOrg;

	@Autowired
	@Qualifier("transformWqxAttachedObjectActivity")
	public TransformWqxAttachedObjectActivity transformWqxAttachedObjectActivity;

	@Autowired
	@Qualifier("truncateWqxAttachedObjectActivity")
	public TruncateWqxAttachedObjectActivity truncateWqxAttachedObjectActivity;

	@Autowired
	@Qualifier("truncateWqxActivityMetricSum")
	public TruncateWqxActivityMetricSum truncateWqxActivityMetricSum;

	@Autowired
	@Qualifier("transformWqxActivityMetricSum")
	public TransformWqxActivityMetricSum transformWqxActivityMetricSum;

	@Autowired
	@Qualifier("dropActivityIndexes")
	public Tasklet dropActivityIndexes;

	@Autowired
	@Qualifier("truncateActivity")
	public Tasklet truncateActivity;

	@Autowired
	@Qualifier("transformActivityWqx")
	public Tasklet transformActivityWqx;

	@Autowired
	@Qualifier("transformActivityStoretw")
	public Tasklet transformActivityStoretw;

	@Autowired
	@Qualifier("buildActivityIndexes")
	public Tasklet buildActivityIndexes;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("activity")
				.start(wqxActivityProject())
				.split(new SimpleAsyncTaskExecutor())
				.add(wqxActivityConductingOrg(), wqxAttachedObjectActivity(), wqxActivityMetricSum(), wqxActivityStart())
				.next(transformActivityWqx())
				.next(transformActivityStoretw())
				.next(buildActivityIndexes())
				.build();
	}

	public Step truncateWqxActivityProject() {
		return stepBuilderFactory.get("truncateWqxActivityProject")
				.tasklet(truncateWqxActivityProject)
				.build();
	}

	public Step transformWqxActivityProject() {
		return stepBuilderFactory.get("transformWqxActivityProject")
				.tasklet(transformWqxActivityProject)
				.build();
	}

	public Step truncateWqxActivityConductingOrg() {
		return stepBuilderFactory.get("truncateWqxActivityConductingOrg")
				.tasklet(truncateWqxActivityConductingOrg)
				.build();
	}

	public Step transformWqxActivityConductingOrg() {
		return stepBuilderFactory.get("transformWqxActivityConductingOrg")
				.tasklet(transformWqxActivityConductingOrg)
				.build();
	}

	public Step truncateWqxAttachedObjectActivity() {
		return stepBuilderFactory.get("truncateWqxAttachedObjectActivity")
				.tasklet(truncateWqxAttachedObjectActivity)
				.build();
	}

	public Step transformWqxAttachedObjectActivity() {
		return stepBuilderFactory.get("transformWqxAttachedObjectActivity")
				.tasklet(transformWqxAttachedObjectActivity)
				.build();
	}

	public Step truncateWqxActivityMetricSum() {
		return stepBuilderFactory.get("truncateWqxActivityMetricSum")
				.tasklet(truncateWqxActivityMetricSum)
				.build();
	}

	public Step transformWqxActivityMetricSum() {
		return stepBuilderFactory.get("transformWqxActivityMetricSum")
				.tasklet(transformWqxActivityMetricSum)
				.build();
	}

	public Step dropActivityIndexes() {
		return stepBuilderFactory.get("dropActivityIndexes")
				.tasklet(dropActivityIndexes)
				.build();
	}

	public Step truncateActivity() {
		return stepBuilderFactory.get("truncateActivity")
				.tasklet(truncateActivity)
				.build();
	}

	public Step transformActivityWqx() {
		return stepBuilderFactory.get("transformActivityWqx")
				.tasklet(transformActivityWqx)
				.build();
	}

	public Step transformActivityStoretw() {
		return stepBuilderFactory.get("transformActivityStoretw")
				.tasklet(transformActivityStoretw)
				.build();
	}

	public Step buildActivityIndexes() {
		return stepBuilderFactory.get("buildActivityIndexes")
				.tasklet(buildActivityIndexes)
				.build();
	}

	public Flow wqxActivityProject() {
		return new FlowBuilder<SimpleFlow>("wqxActivityProject")
				.start(truncateWqxActivityProject())
				.next(transformWqxActivityProject())
				.build();
	}

	public Flow wqxActivityConductingOrg() {
		return new FlowBuilder<SimpleFlow>("wqxActivityConductingOrg")
				.start(truncateWqxActivityConductingOrg())
				.next(transformWqxActivityConductingOrg())
				.build();
	}

	public Flow wqxAttachedObjectActivity() {
		return new FlowBuilder<SimpleFlow>("wqxAttachedObjectActivity")
				.start(truncateWqxAttachedObjectActivity())
				.next(transformWqxAttachedObjectActivity())
				.build();
	}

	public Flow wqxActivityMetricSum() {
		return new FlowBuilder<SimpleFlow>("wqxActivityMetricSum")
				.start(truncateWqxActivityMetricSum())
				.next(transformWqxActivityMetricSum())
				.build();
	}

	public Flow wqxActivityStart() {
		return new FlowBuilder<SimpleFlow>("wqxActivitystart")
				.start(dropActivityIndexes())
				.next(truncateActivity())
				.build();
	}
}
