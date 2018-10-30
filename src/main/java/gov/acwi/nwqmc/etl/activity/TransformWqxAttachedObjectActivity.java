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
public class TransformWqxAttachedObjectActivity implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformWqxAttachedObjectActivity(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */ into wqx_attached_object_activity (org_uid, ref_uid, activity_object_name, activity_object_type)\n" + 
				"select /*+ parallel(4) */\n" + 
				"       org_uid,\n" + 
				"       ref_uid,\n" + 
				"       listagg(atobj_file_name, ';') within group (order by rownum) activity_object_name,\n" + 
				"       listagg(atobj_type, ';') within group (order by rownum) activity_object_type\n" + 
				"  from wqx.attached_object\n" + 
				" where tbl_uid = 3\n" + 
				"    group by org_uid, ref_uid");
		return RepeatStatus.FINISHED;
	}
}
