package gov.acwi.nwqmc.etl.projectObject;

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
public class TransformProjectObjectWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformProjectObjectWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into project_object_swap_storet(data_source_id,\n" + 
				"                                  object_id,\n" + 
				"                                  data_source,\n" + 
				"                                  organization,\n" + 
				"                                  project_identifier,\n" + 
				"                                  object_name,\n" + 
				"                                  object_type,\n" + 
				"                                  object_content)\n" + 
				"select '3',\n" + 
				"       attached_object.atobj_uid,\n" + 
				"       'STORET',\n" + 
				"       organization.org_id organization,\n" + 
				"       project.prj_id project_identifier,\n" + 
				"       attached_object.atobj_file_name,\n" + 
				"       attached_object.atobj_type,\n" + 
				"       attached_object.atobj_content\n" + 
				"  from wqx.attached_object\n" + 
				"       join wqx.organization\n" + 
				"         on attached_object.org_uid = organization.org_uid\n" + 
				"       join wqx.project\n" + 
				"         on attached_object.ref_uid = project.prj_uid\n" + 
				" where attached_object.tbl_uid = 1");
		return RepeatStatus.FINISHED;
	}
}
