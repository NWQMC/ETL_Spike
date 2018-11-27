package gov.acwi.wqp.etl.activity;

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
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
public class ActivityTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("truncateWqxActivityProject")
	private Tasklet truncateWqxActivityProject;

	@Autowired
	@Qualifier("transformWqxActivityProject")
	private Tasklet transformWqxActivityProject;

	@Autowired
	@Qualifier("truncateWqxActivityConductingOrg")
	private Tasklet truncateWqxActivityConductingOrg;

	@Autowired
	@Qualifier("transformWqxActivityConductingOrg")
	private Tasklet transformWqxActivityConductingOrg;

	@Autowired
	@Qualifier("transformWqxAttachedObjectActivity")
	private Tasklet transformWqxAttachedObjectActivity;

	@Autowired
	@Qualifier("truncateWqxAttachedObjectActivity")
	private Tasklet truncateWqxAttachedObjectActivity;

	@Autowired
	@Qualifier("truncateWqxActivityMetricSum")
	private Tasklet truncateWqxActivityMetricSum;

	@Autowired
	@Qualifier("transformWqxActivityMetricSum")
	private Tasklet transformWqxActivityMetricSum;

	@Autowired
	@Qualifier("dropActivityIndexes")
	private Tasklet dropActivityIndexes;

	@Autowired
	@Qualifier("truncateActivity")
	private Tasklet truncateActivity;

	@Autowired
	@Qualifier("transformActivityWqx")
	private Tasklet transformActivityWqx;

	@Autowired
	@Qualifier("transformActivityStoretw")
	private Tasklet transformActivityStoretw;

	@Autowired
	@Qualifier("buildActivityIndexes")
	private Tasklet buildActivityIndexes;

	@Bean
	public Flow activityFlow() {
		return new FlowBuilder<SimpleFlow>("activityFlow")
				.start(wqxActivityProjectFlow())
				.split(new SimpleAsyncTaskExecutor())
				.add(wqxActivityConductingOrgFlow(), wqxAttachedObjectActivityFlow(),
						wqxActivityMetricSumFlow(), wqxActivityStartFlow())
				.next(transformActivityWqxStep())
				.next(transformActivityStoretwStep())
				.next(buildActivityIndexesStep())
				.build();
	}

	@Bean
	public Step truncateWqxActivityProjectStep() {
		return stepBuilderFactory.get("truncateWqxActivityProjectStep")
				.tasklet(truncateWqxActivityProject)
				.build();
	}

	@Bean
	public Step transformWqxActivityProjectStep() {
		return stepBuilderFactory.get("transformWqxActivityProjectStep")
				.tasklet(transformWqxActivityProject)
				.build();
	}

	@Bean
	public Step truncateWqxActivityConductingOrgStep() {
		return stepBuilderFactory.get("truncateWqxActivityConductingOrgStep")
				.tasklet(truncateWqxActivityConductingOrg)
				.build();
	}

	@Bean
	public Step transformWqxActivityConductingOrgStep() {
		return stepBuilderFactory.get("transformWqxActivityConductingOrgStep")
				.tasklet(transformWqxActivityConductingOrg)
				.build();
	}

	@Bean
	public Step truncateWqxAttachedObjectActivityStep() {
		return stepBuilderFactory.get("truncateWqxAttachedObjectActivityStep")
				.tasklet(truncateWqxAttachedObjectActivity)
				.build();
	}

	@Bean
	public Step transformWqxAttachedObjectActivityStep() {
		return stepBuilderFactory.get("transformWqxAttachedObjectActivityStep")
				.tasklet(transformWqxAttachedObjectActivity)
				.build();
	}

	@Bean
	public Step truncateWqxActivityMetricSumStep() {
		return stepBuilderFactory.get("truncateWqxActivityMetricSumStep")
				.tasklet(truncateWqxActivityMetricSum)
				.build();
	}

	@Bean
	public Step transformWqxActivityMetricSumStep() {
		return stepBuilderFactory.get("transformWqxActivityMetricSumStep")
				.tasklet(transformWqxActivityMetricSum)
				.build();
	}

	@Bean
	public Step dropActivityIndexesStep() {
		return stepBuilderFactory.get("dropActivityIndexesStep")
				.tasklet(dropActivityIndexes)
				.build();
	}

	@Bean
	public Step truncateActivityStep() {
		return stepBuilderFactory.get("truncateActivityStep")
				.tasklet(truncateActivity)
				.build();
	}

	@Bean
	public Step transformActivityWqxStep() {
		return stepBuilderFactory.get("transformActivityWqxStep")
				.tasklet(transformActivityWqx)
				.build();
	}

	@Bean
	public Step transformActivityStoretwStep() {
		return stepBuilderFactory.get("transformActivityStoretwStep")
				.tasklet(transformActivityStoretw)
				.build();
	}

	@Bean
	public Step buildActivityIndexesStep() {
		return stepBuilderFactory.get("buildActivityIndexesStep")
				.tasklet(buildActivityIndexes)
				.build();
	}

	@Bean
	public Flow wqxActivityProjectFlow() {
		return new FlowBuilder<SimpleFlow>("wqxActivityProjectFlow")
				.start(truncateWqxActivityProjectStep())
				.next(transformWqxActivityProjectStep())
				.build();
	}

	@Bean
	public Flow wqxActivityConductingOrgFlow() {
		return new FlowBuilder<SimpleFlow>("wqxActivityConductingOrgFlow")
				.start(truncateWqxActivityConductingOrgStep())
				.next(transformWqxActivityConductingOrgStep())
				.build();
	}

	@Bean
	public Flow wqxAttachedObjectActivityFlow() {
		return new FlowBuilder<SimpleFlow>("wqxAttachedObjectActivityFlow")
				.start(truncateWqxAttachedObjectActivityStep())
				.next(transformWqxAttachedObjectActivityStep())
				.build();
	}

	@Bean
	public Flow wqxActivityMetricSumFlow() {
		return new FlowBuilder<SimpleFlow>("wqxActivityMetricSumFlow")
				.start(truncateWqxActivityMetricSumStep())
				.next(transformWqxActivityMetricSumStep())
				.build();
	}

	@Bean
	public Flow wqxActivityStartFlow() {
		return new FlowBuilder<SimpleFlow>("wqxActivitystartFlow")
				.start(dropActivityIndexesStep())
				.next(truncateActivityStep())
				.build();
	}
}
