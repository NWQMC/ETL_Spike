package gov.acwi.nwqmc.etl.result;

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
public class TransformWqxResultTaxonHabit implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformWqxResultTaxonHabit(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into wqx_result_taxon_habit (res_uid, habit_name_list)\n" + 
				"select /*+ parallel(4) */\n" + 
				"       result_taxon_habit.res_uid,\n" + 
				"       listagg(habit.habit_name, ';') within group (order by habit.habit_uid) habit_name_list\n" + 
				"  from wqx.result_taxon_habit\n" + 
				"       left join wqx.habit\n" + 
				"         on result_taxon_habit.habit_uid = habit.habit_uid\n" + 
				"    group by result_taxon_habit.res_uid");
		return RepeatStatus.FINISHED;
	}
}
