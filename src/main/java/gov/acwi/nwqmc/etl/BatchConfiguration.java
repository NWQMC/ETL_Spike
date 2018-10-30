package gov.acwi.nwqmc.etl;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import gov.acwi.nwqmc.etl.activity.ActivityTransformation;
import gov.acwi.nwqmc.etl.activityMetric.ActivityMetricTransformation;
import gov.acwi.nwqmc.etl.activityObject.ActivityObjectTransformation;
import gov.acwi.nwqmc.etl.biologicalHabitatMetric.BiologicalHabitatMetricTransformation;
import gov.acwi.nwqmc.etl.code.CreateCodes;
import gov.acwi.nwqmc.etl.monitoringLocation.MonitoringLocationTransformation;
import gov.acwi.nwqmc.etl.monitoringLocationObject.MonitoringLocationObjectTransformation;
import gov.acwi.nwqmc.etl.orgData.OrgDataTransformation;
import gov.acwi.nwqmc.etl.project.ProjectTransformation;
import gov.acwi.nwqmc.etl.projectMlWeighting.ProjectMlWeightingTransformation;
import gov.acwi.nwqmc.etl.projectObject.ProjectObjectTransformation;
import gov.acwi.nwqmc.etl.resDetectQntLmt.ResDetectQntLmtTransformation;
import gov.acwi.nwqmc.etl.result.ResultTransformation;
import gov.acwi.nwqmc.etl.resultObject.ResultObjectTransformation;
import gov.acwi.nwqmc.etl.summary.CreateSummaries;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public OrgDataTransformation orgDataTransformation;

	@Autowired
	public ProjectTransformation projectTransformation;

	@Autowired
	public ProjectObjectTransformation projectObjectTransformation;

	@Autowired
	public MonitoringLocationTransformation monitoringLocationTransformation;

	@Autowired
	public BiologicalHabitatMetricTransformation biologicalHabitatMetricTransformation;

	@Autowired
	public MonitoringLocationObjectTransformation monitoringLocationObjectTransformation;

	@Autowired
	public ActivityTransformation activityTransformation;

	@Autowired
	public ActivityObjectTransformation activityObjectTransformation;

	@Autowired
	public ActivityMetricTransformation activityMetricTransformation;

	@Autowired
	public ResultTransformation resultTransformation;

	@Autowired
	public ResultObjectTransformation resultObjectTransformation;

	@Autowired
	public ResDetectQntLmtTransformation resDetectQntLmtTransformation;

	@Autowired
	public ProjectMlWeightingTransformation projectMlWeightingTransformation;

	@Autowired
	public CreateSummaries createSummaries;

	@Autowired
	public CreateCodes createCodes;

	@Bean
	public Job wqxEtl(Step dropRiStep, Step addRiStep, Step analyzeStep, Step validateStep,
			Step installStep, Step finalizeStep) {
		return new FlowJobBuilder(jobBuilderFactory.get("WQX_ETL"))
				.start(dropRiStep)
				.next(orgDataTransformation.getFlow())
				.next(projectTransformation.getFlow())
				.next(projectObjectTransformation.getFlow())
				.next(monitoringLocationTransformation.getFlow())
				.next(biologicalHabitatMetricTransformation.getFlow())
				.next(monitoringLocationObjectTransformation.getFlow())
				.next(activityTransformation.getFlow())
				.next(activityObjectTransformation.getFlow())
				.next(activityMetricTransformation.getFlow())
				.next(resultTransformation.getFlow())
				.next(resultObjectTransformation.getFlow())
				.next(resDetectQntLmtTransformation.getFlow())
				.next(projectMlWeightingTransformation.getFlow())
				.next(createSummaries.getFlow())
				.next(createCodes.getFlow())
				.next(addRiStep)
				.next(analyzeStep)
				//TODO activate validate so it works
//				.next(validateStep)
				.next(installStep)
				.next(finalizeStep)
				.build()
				.build();
	}

	@Bean
	public Step dropRiStep(Tasklet dropRi) {
		return stepBuilderFactory.get("dropRi")
				.tasklet(dropRi)
				.build();
	}

	@Bean
	public Step addRiStep(Tasklet addRi) {
		return stepBuilderFactory.get("addRi")
				.tasklet(addRi)
				.build();
	}

	@Bean
	public Step analyzeStep(Tasklet analyze) {
		return stepBuilderFactory.get("analyze")
				.tasklet(analyze)
				.build();
	}

	@Bean
	public Step validateStep(Tasklet validate) {
		return stepBuilderFactory.get("validate")
				.tasklet(validate)
				.build();
	}

	@Bean
	public Step installStep(Tasklet install) {
		return stepBuilderFactory.get("install")
				.tasklet(install)
				.build();
	}

	@Bean
	public Step finalizeStep(Tasklet finalize) {
		return stepBuilderFactory.get("finalize")
				.tasklet(finalize)
				.build();
	}
}
