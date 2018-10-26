package gov.acwi.nwqmc.etl;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import gov.acwi.nwqmc.etl.monitoringLocation.MonitoringLocationTransformation;
import gov.acwi.nwqmc.etl.orgData.OrgDataTransformation;
import gov.acwi.nwqmc.etl.project.ProjectTransformation;
import gov.acwi.nwqmc.etl.projectObject.ProjectObjectTransformation;

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

	@Bean
	public Job taskletJob(Step dropRiStep) {
		SimpleJobBuilder job = jobBuilderFactory.get("taskletJob")
				.start(dropRiStep);

		job = orgDataTransformation.build(job);

		job = projectTransformation.build(job);

		job = projectObjectTransformation.build(job);

		job = monitoringLocationTransformation.build(job);

		return job.build();
	}

	@Bean
	public Step dropRiStep(Tasklet dropRi) {
		return stepBuilderFactory.get("dropRi")
				.tasklet(dropRi)
				.build();
	}

}
