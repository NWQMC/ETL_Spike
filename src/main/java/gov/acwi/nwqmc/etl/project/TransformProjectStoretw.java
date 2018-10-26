package gov.acwi.nwqmc.etl.project;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@StepScope
public class TransformProjectStoretw implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformProjectStoretw(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into project_data_swap_storet (data_source_id,\n" + 
				"                                 project_id,\n" + 
				"                                 data_source,\n" + 
				"                                 organization,\n" + 
				"                                 organization_name,\n" + 
				"                                 project_identifier,\n" + 
				"                                 project_name,\n" + 
				"                                 description,\n" + 
				"                                 sampling_design_type_code,\n" + 
				"                                 qapp_approved_indicator,\n" + 
				"                                 qapp_approval_agency_name\n" + 
				"                                )\n" + 
				"select 3 data_source_id,\n" + 
				"       di_project.pk_isn + 100000000000 project_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       di_org.organization_id organization,\n" + 
				"       di_org.organization_name,\n" + 
				"       di_project.project_cd project_identifier,\n" + 
				"       di_project.project_name,\n" + 
				"       di_project.project_description,\n" + 
				"       di_project.sampling_design_type_cd,\n" + 
				"       di_project.qa_approved qapp_approved_indicator,\n" + 
				"       di_project.qa_approval_agency qapp_approval_agency_name\n" + 
				"  from storetw.di_project\n" + 
				"       join storetw.di_org\n" + 
				"         on di_project.fk_org = di_org.pk_isn\n" + 
				" where di_project.tsmproj_org_id not in (select org_id from wqp_core.storetw_transition) and\n" + 
				"       lnnvl(di_project.source_system = 'WQX')");
		return RepeatStatus.FINISHED;
	}
}
