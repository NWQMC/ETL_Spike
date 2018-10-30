package gov.acwi.nwqmc.etl.activity;

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
public class TransformWqxActivityConductingOrg implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformWqxActivityConductingOrg(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert  /*+ append parallel(4) */ into wqx_activity_conducting_org (act_uid, acorg_name_list)\n" + 
				"select /*+ parallel(4) */\n" + 
				"       act_uid,\n" + 
				"       listagg(acorg_name, ';') within group (order by rownum) acorg_name_list\n" + 
				"  from wqx.activity_conducting_org\n" + 
				"    group by act_uid");
		return RepeatStatus.FINISHED;
	}
}
