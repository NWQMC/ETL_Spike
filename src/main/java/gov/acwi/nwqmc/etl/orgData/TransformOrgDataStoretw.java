package gov.acwi.nwqmc.etl.orgData;

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
public class TransformOrgDataStoretw implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformOrgDataStoretw(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into org_data_swap_storet (data_source_id, data_source, organization_id, organization, organization_name,\n" + 
				"                             organization_description, organization_type)\n" + 
				"select /*+ parallel(4) */ \n" + 
				"       3 data_source_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       pk_isn + 10000000  organization_id,\n" + 
				"       organization_id organization,\n" + 
				"       organization_name,\n" + 
				"       organization_description,\n" + 
				"       organization_type\n" + 
				"  from storetw.di_org\n" + 
				" where source_system is null and\n" +
				"       organization_id not in(select org_id from wqp_core.storetw_transition)");
		return RepeatStatus.FINISHED;
	}
}
