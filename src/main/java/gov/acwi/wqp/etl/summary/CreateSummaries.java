package gov.acwi.wqp.etl.summary;

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

@Configuration
public class CreateSummaries {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("createActivitySum")
	private Tasklet createActivitySum;

	@Autowired
	@Qualifier("createResultSum")
	private Tasklet createResultSum;

	@Autowired
	@Qualifier("createOrgGrouping")
	private Tasklet createOrgGrouping;

	@Autowired
	@Qualifier("createMlGrouping")
	private Tasklet createMlGrouping;

	@Autowired
	@Qualifier("dropOrgSumTemp")
	private Tasklet DropOrgSumTemp;

	@Autowired
	@Qualifier("createOrgSumTemp")
	private Tasklet CreateOrgSumTemp;

	@Autowired
	@Qualifier("dropYearSumTemp")
	private Tasklet DropYearSumTemp;

	@Autowired
	@Qualifier("createYearSumTemp")
	private Tasklet CreateYearSumTemp;

	@Autowired
	@Qualifier("dropOrganizationSumIndexes")
	private Tasklet DropOrganizationSumIndexes;

	@Autowired
	@Qualifier("truncateOrganizationSum")
	private Tasklet TruncateOrganizationSum;

	@Autowired
	@Qualifier("createOrganizationSum")
	private Tasklet createOrganizationSum;

	@Autowired
	@Qualifier("createStationSum")
	private Tasklet createStationSum;

	@Autowired
	@Qualifier("createQwportalSum")
	private Tasklet createQwportalSum;

	@Autowired
	@Qualifier("createSummaryIndexes")
	private Tasklet createSummaryIndexes;

	@Bean
	public Flow createSummariesFlow() {
		return new FlowBuilder<SimpleFlow>("createSummariesFlow")
				.start(createSummaryTablesFlow())
				.next(createSummaryIndexesStep())
				.build();
	}

	@Bean
	public Step createActivitySumStep() {
		return stepBuilderFactory.get("createActivitySumStep")
				.tasklet(createActivitySum)
				.build();
	}

	@Bean
	public Step createResultSumStep() {
		return stepBuilderFactory.get("createResultSumStep")
				.tasklet(createResultSum)
				.build();
	}

	@Bean
	public Step createOrgGroupingStep() {
		return stepBuilderFactory.get("createOrgGroupingStep")
				.tasklet(createOrgGrouping)
				.build();
	}

	@Bean
	public Step createMlGroupingStep() {
		return stepBuilderFactory.get("createMlGroupingStep")
				.tasklet(createMlGrouping)
				.build();
	}

	@Bean
	public Step DropOrgSumTempStep() {
		return stepBuilderFactory.get("DropOrgSumTempStep")
				.tasklet(DropOrgSumTemp)
				.build();
	}

	@Bean
	public Step CreateOrgSumTempStep() {
		return stepBuilderFactory.get("CreateOrgSumTempStep")
				.tasklet(CreateOrgSumTemp)
				.build();
	}

	@Bean
	public Step DropYearSumTempStep() {
		return stepBuilderFactory.get("DropYearSumTempStep")
				.tasklet(DropYearSumTemp)
				.build();
	}

	@Bean
	public Step CreateYearSumTempStep() {
		return stepBuilderFactory.get("CreateYearSumTempStep")
				.tasklet(CreateYearSumTemp)
				.build();
	}

	@Bean
	public Step DropOrganizationSumIndexesStep() {
		return stepBuilderFactory.get("DropOrganizationSumIndexesStep")
				.tasklet(DropOrganizationSumIndexes)
				.build();
	}

	@Bean
	public Step TruncateOrganizationSumStep() {
		return stepBuilderFactory.get("TruncateOrganizationSumStep")
				.tasklet(TruncateOrganizationSum)
				.build();
	}

	@Bean
	public Step createOrganizationSumStep() {
		return stepBuilderFactory.get("createOrganizationSumStep")
				.tasklet(createOrganizationSum)
				.build();
	}

	@Bean
	public Step createStationSumStep() {
		return stepBuilderFactory.get("createStationStep")
				.tasklet(createStationSum)
				.build();
	}

	@Bean
	public Step createQwportalSumStep() {
		return stepBuilderFactory.get("createQwportalSumStep")
				.tasklet(createQwportalSum)
				.build();
	}

	@Bean
	public Step createSummaryIndexesStep() {
		return stepBuilderFactory.get("createSummaryIndexesStep")
				.tasklet(createSummaryIndexes)
				.build();
	}

	@Bean
	public Flow createSummaryTablesFlow() {
		return new FlowBuilder<SimpleFlow>("createSummaryTablesFlow")
				.start(createActivitySumStep())
				.next(createResultSumStep())
				.next(createOrgGroupingStep())
				.next(createMlGroupingStep())
				.next(createOrganizationSumFlow())
				.next(createStationSumStep())
				.next(createQwportalSumStep())
				.build();
	}

	@Bean
	public Flow createOrganizationSumFlow() {
		return new FlowBuilder<SimpleFlow>("createOrganizationSumFlow")
				.start(DropOrgSumTempStep())
				.next(CreateOrgSumTempStep())
				.next(DropYearSumTempStep())
				.next(CreateYearSumTempStep())
				.next(DropOrganizationSumIndexesStep())
				.next(TruncateOrganizationSumStep())
				.next(createOrganizationSumStep())
				.build();
	}

}
