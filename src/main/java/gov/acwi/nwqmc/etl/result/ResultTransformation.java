package gov.acwi.nwqmc.etl.result;

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
public class ResultTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("truncateWqpNemiEpaCrosswalk")
	private TruncateWqpNemiEpaCrosswalk truncateWqpNemiEpaCrosswalk;

	@Autowired
	@Qualifier("transformWqpNemiEpaCrosswalk")
	private TransformWqpNemiEpaCrosswalk transformWqpNemiEpaCrosswalk;

	@Autowired
	@Qualifier("truncateWqxAnalyticalMethod")
	private TruncateWqxAnalyticalMethod truncateWqxAnalyticalMethod;

	@Autowired
	@Qualifier("transformWqxAnalyticalMethod")
	private TransformWqxAnalyticalMethod transformWqxAnalyticalMethod;

	@Autowired
	@Qualifier("truncateWqxRDetectQntLmt")
	private TruncateWqxRDetectQntLmt truncateWqxRDetectQntLmt;

	@Autowired
	@Qualifier("transformWqxRDetectQntLmt")
	private TransformWqxRDetectQntLmt transformWqxRDetectQntLmt;

	@Autowired
	@Qualifier("truncateWqxDetectionQuantLimit")
	private TruncateWqxDetectionQuantLimit truncateWqxDetectionQuantLimit;

	@Autowired
	@Qualifier("transformWqxDetectionQuantLimit")
	private TransformWqxDetectionQuantLimit transformWqxDetectionQuantLimit;

	@Autowired
	@Qualifier("truncateWqxResultTaxonHabit")
	private TruncateWqxResultTaxonHabit truncateWqxResultTaxonHabit;

	@Autowired
	@Qualifier("transformWqxResultTaxonHabit")
	private TransformWqxResultTaxonHabit transformWqxResultTaxonHabit;

	@Autowired
	@Qualifier("truncateWqxResultTaxonFeedingGroup")
	private TruncateWqxResultTaxonFeedingGroup truncateWqxResultTaxonFeedingGroup;

	@Autowired
	@Qualifier("transformWqxResultTaxonFeedingGroup")
	private TransformWqxResultTaxonFeedingGroup transformWqxResultTaxonFeedingGroup;

	@Autowired
	@Qualifier("truncateWqxAttachedObjectResult")
	private TruncateWqxAttachedObjectResult truncateWqxAttachedObjectResult;

	@Autowired
	@Qualifier("transformWqxAttachedObjectResult")
	private TransformWqxAttachedObjectResult transformWqxAttachedObjectResult;

	@Autowired
	@Qualifier("truncateWqxResultLabSamplePrepSum")
	private TruncateWqxResultLabSamplePrepSum truncateWqxResultLabSamplePrepSum;

	@Autowired
	@Qualifier("transformWqxResultLabSamplePrepSum")
	private TransformWqxResultLabSamplePrepSum transformWqxResultLabSamplePrepSum;

	@Autowired
	@Qualifier("truncateWqxResultFrequencyClass")
	private TruncateWqxResultFrequencyClass truncateWqxResultFrequencyClass;

	@Autowired
	@Qualifier("transformWqxResultFrequencyClass")
	private TransformWqxResultFrequencyClass transformWqxResultFrequencyClass;

	@Autowired
	@Qualifier("dropResultIndexes")
	private Tasklet dropResultIndexes;

	@Autowired
	@Qualifier("truncateResult")
	private Tasklet truncateResult;

	@Autowired
	@Qualifier("transformResultWqx")
	private Tasklet transformResultWqx;

	@Autowired
	@Qualifier("transformResultStoretw")
	private Tasklet transformResultStoretw;

	@Autowired
	@Qualifier("buildResultIndexes")
	private Tasklet buildResultIndexes;

	@Bean
	public Flow resultFlow() {
		return new FlowBuilder<SimpleFlow>("resultFlow")
				.start(wqpNemiEpaCrosswalkFlow())
				.split(new SimpleAsyncTaskExecutor())
				.add(wqxAnalyticalMethodFlow(), wqxRDetectQntLmtFlow(),
						wqxResultTaxonHabitFlow(), wqxResultTaxonFeedingGroupFlow(),
						wqxAttachedObjectResultFlow(), wqxResultLabSamplePrepSumFlow(),
						wqxResultFrequencyClassFlow(), wqxResultStartFlow())
				.next(transformResultWqxStep())
				.next(transformResultStoretwStep())
				.next(buildResultIndexesStep())
				.build();
	}

	@Bean
	public Step truncateWqpNemiEpaCrosswalkStep() {
		return stepBuilderFactory.get("truncateWqpNemiEpaCrosswalkStep")
				.tasklet(truncateWqpNemiEpaCrosswalk)
				.build();
	}

	@Bean
	public Step transformWqpNemiEpaCrosswalkStep() {
		return stepBuilderFactory.get("transformWqpNemiEpaCrosswalkStep")
				.tasklet(transformWqpNemiEpaCrosswalk)
				.build();
	}

	@Bean
	public Step truncateWqxAnalyticalMethodStep() {
		return stepBuilderFactory.get("truncateWqxAnalyticalMethodStep")
				.tasklet(truncateWqxAnalyticalMethod)
				.build();
	}

	@Bean
	public Step transformWqxAnalyticalMethodStep() {
		return stepBuilderFactory.get("transformWqxAnalyticalMethodStep")
				.tasklet(transformWqxAnalyticalMethod)
				.build();
	}

	@Bean
	public Step truncateWqxRDetectQntLmtStep() {
		return stepBuilderFactory.get("truncateWqxRDetectQntLmtStep")
				.tasklet(truncateWqxRDetectQntLmt)
				.build();
	}

	@Bean
	public Step transformWqxRDetectQntLmtStep() {
		return stepBuilderFactory.get("transformWqxRDetectQntLmtStep")
				.tasklet(transformWqxRDetectQntLmt)
				.build();
	}

	@Bean
	public Step truncateWqxDetectionQuantLimitStep() {
		return stepBuilderFactory.get("truncateWqxDetectionQuantLimitStep")
				.tasklet(truncateWqxDetectionQuantLimit)
				.build();
	}

	@Bean
	public Step transformWqxDetectionQuantLimitStep() {
		return stepBuilderFactory.get("transformWqxDetectionQuantLimitStep")
				.tasklet(transformWqxDetectionQuantLimit)
				.build();
	}

	@Bean
	public Step truncateWqxResultTaxonHabitStep() {
		return stepBuilderFactory.get("truncateWqxResultTaxonHabitStep")
				.tasklet(truncateWqxResultTaxonHabit)
				.build();
	}

	@Bean
	public Step transformWqxResultTaxonHabitStep() {
		return stepBuilderFactory.get("transformWqxResultTaxonHabitStep")
				.tasklet(transformWqxResultTaxonHabit)
				.build();
	}

	@Bean
	public Step truncateWqxResultTaxonFeedingGroupStep() {
		return stepBuilderFactory.get("truncateWqxResultTaxonFeedingGroupStep")
				.tasklet(truncateWqxResultTaxonFeedingGroup)
				.build();
	}

	@Bean
	public Step transformWqxResultTaxonFeedingGroupStep() {
		return stepBuilderFactory.get("transformWqxResultTaxonFeedingGroupStep")
				.tasklet(transformWqxResultTaxonFeedingGroup)
				.build();
	}

	@Bean
	public Step truncateWqxAttachedObjectResultStep() {
		return stepBuilderFactory.get("truncateWqxAttachedObjectResultStep")
				.tasklet(truncateWqxAttachedObjectResult)
				.build();
	}

	@Bean
	public Step transformWqxAttachedObjectResultStep() {
		return stepBuilderFactory.get("transformWqxAttachedObjectResultStep")
				.tasklet(transformWqxAttachedObjectResult)
				.build();
	}

	@Bean
	public Step truncateWqxResultLabSamplePrepSumStep() {
		return stepBuilderFactory.get("truncateWqxResultLabSamplePrepSumStep")
				.tasklet(truncateWqxResultLabSamplePrepSum)
				.build();
	}

	@Bean
	public Step transformWqxResultLabSamplePrepSumStep() {
		return stepBuilderFactory.get("transformWqxResultLabSamplePrepSumStep")
				.tasklet(transformWqxResultLabSamplePrepSum)
				.build();
	}

	@Bean
	public Step truncateWqxResultFrequencyClassStep() {
		return stepBuilderFactory.get("truncateWqxResultFrequencyClassStep")
				.tasklet(truncateWqxResultFrequencyClass)
				.build();
	}

	@Bean
	public Step transformWqxResultFrequencyClassStep() {
		return stepBuilderFactory.get("transformWqxResultFrequencyClassStep")
				.tasklet(transformWqxResultFrequencyClass)
				.build();
	}

	@Bean
	public Step dropResultIndexesStep() {
		return stepBuilderFactory.get("dropResultIndexesStep")
				.tasklet(dropResultIndexes)
				.build();
	}

	@Bean
	public Step truncateResultStep() {
		return stepBuilderFactory.get("truncateResultStep")
				.tasklet(truncateResult)
				.build();
	}

	@Bean
	public Step transformResultWqxStep() {
		return stepBuilderFactory.get("transformResultWqxStep")
				.tasklet(transformResultWqx)
				.build();
	}

	@Bean
	public Step transformResultStoretwStep() {
		return stepBuilderFactory.get("transformResultStoretwStep")
				.tasklet(transformResultStoretw)
				.build();
	}

	@Bean
	public Step buildResultIndexesStep() {
		return stepBuilderFactory.get("buildResultIndexesStep")
				.tasklet(buildResultIndexes)
				.build();
	}

	@Bean
	public Flow wqpNemiEpaCrosswalkFlow() {
		return new FlowBuilder<SimpleFlow>("wqpNemiEpaCrosswalkFlow")
				.start(truncateWqpNemiEpaCrosswalkStep())
				//TODO mock database link - or flip to real batch ItemReader/ItemProcessor/ItemWriter
//				.next(transformWqpNemiEpaCrosswalk())
				.build();
	}

	@Bean
	public Flow wqxAnalyticalMethodFlow() {
		return new FlowBuilder<SimpleFlow>("wqxAnalyticalMethodFlow")
				.start(truncateWqxAnalyticalMethodStep())
				.next(transformWqxAnalyticalMethodStep())
				.build();
	}

	@Bean
	public Flow wqxRDetectQntLmtFlow() {
		return new FlowBuilder<SimpleFlow>("wqxRDetectQntLmtFlow")
				.start(truncateWqxRDetectQntLmtStep())
				.next(transformWqxRDetectQntLmtStep())
				.next(truncateWqxDetectionQuantLimitStep())
				.next(transformWqxDetectionQuantLimitStep())
				.build();
	}

	@Bean
	public Flow wqxResultTaxonHabitFlow() {
		return new FlowBuilder<SimpleFlow>("wqxResultTaxonHabitFlow")
				.start(truncateWqxResultTaxonHabitStep())
				.next(transformWqxResultTaxonHabitStep())
				.build();
	}

	@Bean
	public Flow wqxResultTaxonFeedingGroupFlow() {
		return new FlowBuilder<SimpleFlow>("wqxResultTaxonFeedingGroupFlow")
				.start(truncateWqxResultTaxonFeedingGroupStep())
				.next(transformWqxResultTaxonFeedingGroupStep())
				.build();
	}

	@Bean
	public Flow wqxAttachedObjectResultFlow() {
		return new FlowBuilder<SimpleFlow>("wqxAttachedObjectResultFlow")
				.start(truncateWqxAttachedObjectResultStep())
				.next(transformWqxAttachedObjectResultStep())
				.build();
	}

	@Bean
	public Flow wqxResultLabSamplePrepSumFlow() {
		return new FlowBuilder<SimpleFlow>("wqxResultLabSamplePrepSumFlow")
				.start(truncateWqxResultLabSamplePrepSumStep())
				.next(transformWqxResultLabSamplePrepSumStep())
				.build();
	}

	@Bean
	public Flow wqxResultFrequencyClassFlow() {
		return new FlowBuilder<SimpleFlow>("wqxResultFrequencyClassFlow")
				.start(truncateWqxResultFrequencyClassStep())
				.next(transformWqxResultFrequencyClassStep())
				.build();
	}

	@Bean
	public Flow wqxResultStartFlow() {
		return new FlowBuilder<SimpleFlow>("wqxResultStartFlow")
				.start(dropResultIndexesStep())
				.next(truncateResultStep())
				.build();
	}
}
