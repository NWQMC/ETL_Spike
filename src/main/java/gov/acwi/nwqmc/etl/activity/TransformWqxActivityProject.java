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
public class TransformWqxActivityProject implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformWqxActivityProject(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */ into wqx_activity_project (act_uid, project_id_list, project_name_list)\n" + 
				"select /*+ parallel(4) */\n" + 
				"       activity_project.act_uid,\n" + 
				"       listagg(project.prj_id, ';') within group (order by project.prj_id) project_id_list,\n" + 
				"       listagg(project.prj_id, ';') within group (order by project.prj_id) project_name_list\n" + 
				"--values too long       listagg(project.prj_name, ';') within group (order by project.prj_id) project_name_list\n" + 
				"--does not run on wqx        rtrim(clobagg(project.prj_name || '; '), '; ') project_name_list\n" + 
				"       from wqx.activity_project\n" + 
				"       left join wqx.project\n" + 
				"         on activity_project.prj_uid = project.prj_uid\n" + 
				"    group by activity_project.act_uid");
		return RepeatStatus.FINISHED;
	}
}
