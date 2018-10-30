package gov.acwi.nwqmc.etl.activityObject;

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
public class TransformActivityObjectWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformActivityObjectWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into activity_object_swap_storet (data_source_id,\n" + 
				"                                    object_id,\n" + 
				"                                    data_source,\n" + 
				"                                    activity_id,\n" + 
				"                                    organization,\n" + 
				"                                    activity,\n" + 
				"                                    object_name,\n" + 
				"                                    object_type,\n" + 
				"                                    object_content)\n" + 
				"select '3' data_source_id,\n" + 
				"       atobj_uid object_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       ref_uid,\n" + 
				"       activity_swap_storet.organization,\n" + 
				"       activity_swap_storet.activity,\n" + 
				"       atobj_file_name object_name,\n" + 
				"       atobj_type object_type,\n" + 
				"       atobj_content object_content\n" + 
				"  from wqx.attached_object\n" + 
				"       join activity_swap_storet\n" + 
				"         on attached_object.ref_uid = activity_swap_storet.activity_id\n" + 
				" where tbl_uid = 3");
		return RepeatStatus.FINISHED;
	}
}
