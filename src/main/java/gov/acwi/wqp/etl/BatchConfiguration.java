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
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	@Qualifier("orgDataFlow")
	private Flow orgDataFlow;

	@Autowired
	@Qualifier("databaseSetupFlow")
	private Flow databaseSetupFlow;

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
	@Qualifier("biologicalHabitatMetricFlow")
	private Flow biologicalHabitatMetricFlow;

	@Autowired
	@Qualifier("monitoringLocationObjectFlow")
	private Flow monitoringLocationObjectFlow;

	@Autowired
	@Qualifier("activityFlow")
	private Flow activityFlow;

	@Autowired
	@Qualifier("activityObjectFlow")
	private Flow activityObjectFlow;

	@Autowired
	@Qualifier("activityMetricFlow")
	private Flow activityMetricFlow;

	@Autowired
	@Qualifier("resultFlow")
	private Flow resultFlow;

	@Autowired
	@Qualifier("resultObjectFlow")
	private Flow resultObjectFlow;

	@Autowired
	@Qualifier("resDetectQntLmtFlow")
	private Flow resDetectQntLmtFlow;

	@Autowired
	@Qualifier("projectMlWeightingFlow")
	private Flow projectMlWeightingFlow;

	@Autowired
	@Qualifier("createSummariesFlow")
	private Flow createSummariesFlow;

	@Autowired
	@Qualifier("createCodesFlow")
	private Flow createCodesFlow;

	@Autowired
	@Qualifier("databaseFinalizeFlow")
	private Flow databaseFinalizeFlow;

	@Bean
	public Job wqxEtl() {
		return jobBuilderFactory.get("WQX_ETL")
				.start(databaseSetupFlow)
				.next(orgDataFlow)
				.next(projectDataFlow)
				.next(projectObjectFlow)
				.next(monitoringLocationFlow)
				.next(biologicalHabitatMetricFlow)
				.next(monitoringLocationObjectFlow)
				.next(activityFlow)
				.next(activityObjectFlow)
				.next(activityMetricFlow)
				.next(resultFlow)
				.next(resultObjectFlow)
				.next(resDetectQntLmtFlow)
				.next(projectMlWeightingFlow)
				.next(createSummariesFlow)
				.next(createCodesFlow)
				.next(databaseFinalizeFlow)
				.build() // build the flow
				.build(); // build the job
	}

}
