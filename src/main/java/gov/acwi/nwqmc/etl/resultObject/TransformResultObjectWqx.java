package gov.acwi.nwqmc.etl.resultObject;

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
public class TransformResultObjectWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformResultObjectWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into result_object_swap_storet (data_source_id,\n" + 
				"                                  object_id,\n" + 
				"                                  data_source,\n" + 
				"                                  organization,\n" + 
				"                                  activity_id,\n" + 
				"                                  activity,\n" + 
				"                                  result_id,\n" + 
				"                                  object_name,\n" + 
				"                                  object_type,\n" + 
				"                                  object_content)\n" + 
				"select '3' data_source_id,\n" + 
				"       attached_object.atobj_uid object_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       result_swap_storet.organization,\n" + 
				"       result_swap_storet.activity_id,\n" + 
				"       result_swap_storet.activity,\n" + 
				"       attached_object.ref_uid,\n" + 
				"       attached_object.atobj_file_name object_name,\n" + 
				"       attached_object.atobj_type object_type,\n" + 
				"       attached_object.atobj_content object_content\n" + 
				"  from wqx.attached_object\n" + 
				"       join result_swap_storet\n" + 
				"         on attached_object.ref_uid = result_swap_storet.result_id\n" + 
				" where attached_object.tbl_uid = 5");
		return RepeatStatus.FINISHED;
	}
}
