package gov.acwi.wqp.etl;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	ConcurrentDbStepsUtil concurrentDbStepsUtil;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	@Qualifier("orgDataFlow")
	private Flow orgDataFlow;

	@Autowired
	@Qualifier("projectDataFlow")
	private Flow projectDataFlow;

	@Autowired
	@Qualifier("projectObjectFlow")
	private Flow projectObjectFlow;

	@Autowired
	@Qualifier("monitoringLocationFlow")
	private Flow monitoringLocationFlow;

	@Autowired
	@Qualifier("monitoringLocationObjectFlow")
	private Flow monitoringLocationObjectFlow;

	@Autowired
	@Qualifier("biologicalHabitatMetricFlow")
	private Flow biologicalHabitatMetricFlow;

	@Autowired
	@Qualifier("activityFlow")
	private Flow activityFlow;

	@Autowired
	@Qualifier("activityMetricFlow")
	private Flow activityMetricFlow;

	@Autowired
	@Qualifier("activityObjectFlow")
	private Flow activityObjectFlow;

	@Autowired
	@Qualifier("resultFlow")
	private Flow resultFlow;

	@Autowired
	@Qualifier("resultObjectFlow")
	private Flow resultObjectFlow;

	@Autowired
	@Qualifier("resDetectQntLimitFlow")
	private Flow resDetectQntLimitFlow;

	@Autowired
	@Qualifier("projectMLWeightingFlow")
	private Flow projectMLWeightingFlow;
	

	@Autowired
	@Qualifier(EtlConstantUtils.CREATE_SUMMARIES_FLOW)
	private Flow createSummariesFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.CREATE_LOOKUP_CODES_FLOW)
	private Flow createLookupCodesFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.CREATE_DATABASE_FINALIZE_FLOW)
	private Flow databaseFinalizeFlow;

	@Bean
	public Job wqxEtl() {
		return jobBuilderFactory.get("WQP_STORET_WQX_ETL")
				.listener(concurrentDbStepsUtil) /* Required to shutdown the Threadpool, otherwise it hangs on completion */
				.start(orgDataFlow)
				.next(projectDataFlow)
				.next(projectObjectFlow)
				.next(monitoringLocationFlow)
				.next(monitoringLocationObjectFlow)
				.next(biologicalHabitatMetricFlow)
				.next(activityFlow)
				.next(activityMetricFlow)
				.next(activityObjectFlow)
				.next(resultFlow)
				.next(resultObjectFlow)
				.next(resDetectQntLimitFlow)
				.next(projectMLWeightingFlow)
				.next(createSummariesFlow)
				.next(createLookupCodesFlow)
				.next(databaseFinalizeFlow)
				.build()
				.build();
	}
}
