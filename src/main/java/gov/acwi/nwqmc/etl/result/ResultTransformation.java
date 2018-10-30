package gov.acwi.nwqmc.etl.result;

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
public class ResultTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("truncateWqpNemiEpaCrosswalk")
	public TruncateWqpNemiEpaCrosswalk truncateWqpNemiEpaCrosswalk;

	@Autowired
	@Qualifier("transformWqpNemiEpaCrosswalk")
	public TransformWqpNemiEpaCrosswalk transformWqpNemiEpaCrosswalk;

	@Autowired
	@Qualifier("truncateWqxAnalyticalMethod")
	public TruncateWqxAnalyticalMethod truncateWqxAnalyticalMethod;

	@Autowired
	@Qualifier("transformWqxAnalyticalMethod")
	public TransformWqxAnalyticalMethod transformWqxAnalyticalMethod;

	@Autowired
	@Qualifier("truncateWqxRDetectQntLmt")
	public TruncateWqxRDetectQntLmt truncateWqxRDetectQntLmt;

	@Autowired
	@Qualifier("transformWqxRDetectQntLmt")
	public TransformWqxRDetectQntLmt transformWqxRDetectQntLmt;

	@Autowired
	@Qualifier("truncateWqxDetectionQuantLimit")
	public TruncateWqxDetectionQuantLimit truncateWqxDetectionQuantLimit;

	@Autowired
	@Qualifier("transformWqxDetectionQuantLimit")
	public TransformWqxDetectionQuantLimit transformWqxDetectionQuantLimit;

	@Autowired
	@Qualifier("truncateWqxResultTaxonHabit")
	public TruncateWqxResultTaxonHabit truncateWqxResultTaxonHabit;

	@Autowired
	@Qualifier("transformWqxResultTaxonHabit")
	public TransformWqxResultTaxonHabit transformWqxResultTaxonHabit;

	@Autowired
	@Qualifier("truncateWqxResultTaxonFeedingGroup")
	public TruncateWqxResultTaxonFeedingGroup truncateWqxResultTaxonFeedingGroup;

	@Autowired
	@Qualifier("transformWqxResultTaxonFeedingGroup")
	public TransformWqxResultTaxonFeedingGroup transformWqxResultTaxonFeedingGroup;

	@Autowired
	@Qualifier("truncateWqxAttachedObjectResult")
	public TruncateWqxAttachedObjectResult truncateWqxAttachedObjectResult;

	@Autowired
	@Qualifier("transformWqxAttachedObjectResult")
	public TransformWqxAttachedObjectResult transformWqxAttachedObjectResult;

	@Autowired
	@Qualifier("truncateWqxResultLabSamplePrepSum")
	public TruncateWqxResultLabSamplePrepSum truncateWqxResultLabSamplePrepSum;

	@Autowired
	@Qualifier("transformWqxResultLabSamplePrepSum")
	public TransformWqxResultLabSamplePrepSum transformWqxResultLabSamplePrepSum;

	@Autowired
	@Qualifier("truncateWqxResultFrequencyClass")
	public TruncateWqxResultFrequencyClass truncateWqxResultFrequencyClass;

	@Autowired
	@Qualifier("transformWqxResultFrequencyClass")
	public TransformWqxResultFrequencyClass transformWqxResultFrequencyClass;

	@Autowired
	@Qualifier("dropResultIndexes")
	public Tasklet dropResultIndexes;

	@Autowired
	@Qualifier("truncateResult")
	public Tasklet truncateResult;

	@Autowired
	@Qualifier("transformResultWqx")
	public Tasklet transformResultWqx;

	@Autowired
	@Qualifier("transformResultStoretw")
	public Tasklet transformResultStoretw;

	@Autowired
	@Qualifier("buildResultIndexes")
	public Tasklet buildResultIndexes;

	public Flow getFlow() {
		return new FlowBuilder<SimpleFlow>("result")
				.start(wqpNemiEpaCrosswalk())
				.split(new SimpleAsyncTaskExecutor())
				.add(wqxAnalyticalMethod(), wqxRDetectQntLmt(), wqxResultTaxonHabit(),
						wqxResultTaxonFeedingGroup(), wqxAttachedObjectResult(), wqxResultLabSamplePrepSum(),
						wqxResultFrequencyClass(), wqxResultStart())
				.next(transformResultWqx())
				.next(transformResultStoretw())
				.next(buildResultIndexes())
				.build();
	}

	public Step truncateWqpNemiEpaCrosswalk() {
		return stepBuilderFactory.get("truncateWqpNemiEpaCrosswalk")
				.tasklet(truncateWqpNemiEpaCrosswalk)
				.build();
	}

	public Step transformWqpNemiEpaCrosswalk() {
		return stepBuilderFactory.get("transformWqpNemiEpaCrosswalk")
				.tasklet(transformWqpNemiEpaCrosswalk)
				.build();
	}

	public Step truncateWqxAnalyticalMethod() {
		return stepBuilderFactory.get("truncateWqxAnalyticalMethod")
				.tasklet(truncateWqxAnalyticalMethod)
				.build();
	}

	public Step transformWqxAnalyticalMethod() {
		return stepBuilderFactory.get("transformWqxAnalyticalMethod")
				.tasklet(transformWqxAnalyticalMethod)
				.build();
	}

	public Step truncateWqxRDetectQntLmt() {
		return stepBuilderFactory.get("truncateWqxRDetectQntLmt")
				.tasklet(truncateWqxRDetectQntLmt)
				.build();
	}

	public Step transformWqxRDetectQntLmt() {
		return stepBuilderFactory.get("transformWqxRDetectQntLmt")
				.tasklet(transformWqxRDetectQntLmt)
				.build();
	}

	public Step truncateWqxDetectionQuantLimit() {
		return stepBuilderFactory.get("truncateWqxDetectionQuantLimit")
				.tasklet(truncateWqxDetectionQuantLimit)
				.build();
	}

	public Step transformWqxDetectionQuantLimit() {
		return stepBuilderFactory.get("transformWqxDetectionQuantLimit")
				.tasklet(transformWqxDetectionQuantLimit)
				.build();
	}

	public Step truncateWqxResultTaxonHabit() {
		return stepBuilderFactory.get("truncateWqxResultTaxonHabit")
				.tasklet(truncateWqxResultTaxonHabit)
				.build();
	}

	public Step transformWqxResultTaxonHabit() {
		return stepBuilderFactory.get("transformWqxResultTaxonHabit")
				.tasklet(transformWqxResultTaxonHabit)
				.build();
	}

	public Step truncateWqxResultTaxonFeedingGroup() {
		return stepBuilderFactory.get("truncateWqxResultTaxonFeedingGroup")
				.tasklet(truncateWqxResultTaxonFeedingGroup)
				.build();
	}

	public Step transformWqxResultTaxonFeedingGroup() {
		return stepBuilderFactory.get("transformWqxResultTaxonFeedingGroup")
				.tasklet(transformWqxResultTaxonFeedingGroup)
				.build();
	}

	public Step truncateWqxAttachedObjectResult() {
		return stepBuilderFactory.get("truncateWqxAttachedObjectResult")
				.tasklet(truncateWqxAttachedObjectResult)
				.build();
	}

	public Step transformWqxAttachedObjectResult() {
		return stepBuilderFactory.get("transformWqxAttachedObjectResult")
				.tasklet(transformWqxAttachedObjectResult)
				.build();
	}

	public Step truncateWqxResultLabSamplePrepSum() {
		return stepBuilderFactory.get("truncateWqxResultLabSamplePrepSum")
				.tasklet(truncateWqxResultLabSamplePrepSum)
				.build();
	}

	public Step transformWqxResultLabSamplePrepSum() {
		return stepBuilderFactory.get("transformWqxResultLabSamplePrepSum")
				.tasklet(transformWqxResultLabSamplePrepSum)
				.build();
	}

	public Step truncateWqxResultFrequencyClass() {
		return stepBuilderFactory.get("truncateWqxResultFrequencyClass")
				.tasklet(truncateWqxResultFrequencyClass)
				.build();
	}

	public Step transformWqxResultFrequencyClass() {
		return stepBuilderFactory.get("transformWqxResultFrequencyClass")
				.tasklet(transformWqxResultFrequencyClass)
				.build();
	}

	public Step dropResultIndexes() {
		return stepBuilderFactory.get("dropResultIndexes")
				.tasklet(dropResultIndexes)
				.build();
	}

	public Step truncateResult() {
		return stepBuilderFactory.get("truncateResult")
				.tasklet(truncateResult)
				.build();
	}

	public Step transformResultWqx() {
		return stepBuilderFactory.get("transformResultWqx")
				.tasklet(transformResultWqx)
				.build();
	}

	public Step transformResultStoretw() {
		return stepBuilderFactory.get("transformResultStoretw")
				.tasklet(transformResultStoretw)
				.build();
	}

	public Step buildResultIndexes() {
		return stepBuilderFactory.get("buildResultIndexes")
				.tasklet(buildResultIndexes)
				.build();
	}

	public Flow wqpNemiEpaCrosswalk() {
		return new FlowBuilder<SimpleFlow>("wqpNemiEpaCrosswalk")
				.start(truncateWqpNemiEpaCrosswalk())
				//TODO mock database link - or flip to real batch ItemReader/ItemProcessor/ItemWriter
//				.next(transformWqpNemiEpaCrosswalk())
				.build();
	}

	public Flow wqxAnalyticalMethod() {
		return new FlowBuilder<SimpleFlow>("wqxAnalyticalMethod")
				.start(truncateWqxAnalyticalMethod())
				.next(transformWqxAnalyticalMethod())
				.build();
	}

	public Flow wqxRDetectQntLmt() {
		return new FlowBuilder<SimpleFlow>("wqxRDetectQntLmt")
				.start(truncateWqxRDetectQntLmt())
				.next(transformWqxRDetectQntLmt())
				.next(truncateWqxDetectionQuantLimit())
				.next(transformWqxDetectionQuantLimit())
				.build();
	}

	public Flow wqxResultTaxonHabit() {
		return new FlowBuilder<SimpleFlow>("wqxResultTaxonHabit")
				.start(truncateWqxResultTaxonHabit())
				.next(transformWqxResultTaxonHabit())
				.build();
	}

	public Flow wqxResultTaxonFeedingGroup() {
		return new FlowBuilder<SimpleFlow>("wqxResultTaxonFeedingGroup")
				.start(truncateWqxResultTaxonFeedingGroup())
				.next(transformWqxResultTaxonFeedingGroup())
				.build();
	}

	public Flow wqxAttachedObjectResult() {
		return new FlowBuilder<SimpleFlow>("wqxAttachedObjectResult")
				.start(truncateWqxAttachedObjectResult())
				.next(transformWqxAttachedObjectResult())
				.build();
	}

	public Flow wqxResultLabSamplePrepSum() {
		return new FlowBuilder<SimpleFlow>("wqxResultLabSamplePrepSum")
				.start(truncateWqxResultLabSamplePrepSum())
				.next(transformWqxResultLabSamplePrepSum())
				.build();
	}

	public Flow wqxResultFrequencyClass() {
		return new FlowBuilder<SimpleFlow>("wqxResultFrequencyClass")
				.start(truncateWqxResultFrequencyClass())
				.next(transformWqxResultFrequencyClass())
				.build();
	}

	public Flow wqxResultStart() {
		return new FlowBuilder<SimpleFlow>("wqxResultstart")
				.start(dropResultIndexes())
				.next(truncateResult())
				.build();
	}
}
