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
public class TransformWqxResultFrequencyClass implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformWqxResultFrequencyClass(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into wqx_result_frequency_class (res_uid,\n" + 
				"                                   one_fcdsc_name, one_msunt_cd, one_fcdsc_lower_bound, one_fcdsc_upper_bound,\n" + 
				"                                   two_fcdsc_name, two_msunt_cd, two_fcdsc_lower_bound, two_fcdsc_upper_bound,\n" + 
				"                                   three_fcdsc_name, three_msunt_cd, three_fcdsc_lower_bound, three_fcdsc_upper_bound)\n" + 
				"select /*+ parallel(4) */\n" + 
				"       *\n" + 
				"  from (select result_frequency_class.res_uid,\n" + 
				"               row_number() over (partition by res_uid order by fcdsc_name) pos,\n" + 
				"               frequency_class_descriptor.fcdsc_name,\n" + 
				"               measurement_unit.msunt_cd,\n" + 
				"               result_frequency_class.fcdsc_lower_bound,\n" + 
				"               result_frequency_class.fcdsc_upper_bound\n" + 
				"          from wqx.result_frequency_class\n" + 
				"               left join wqx.frequency_class_descriptor\n" + 
				"                 on result_frequency_class.fcdsc_uid = frequency_class_descriptor.fcdsc_uid\n" + 
				"               left join wqx.measurement_unit\n" + 
				"                 on result_frequency_class.msunt_uid = measurement_unit.msunt_uid\n" + 
				"       )\n" + 
				"pivot (\n" + 
				"       min(fcdsc_name) fcdsc_name,\n" + 
				"       min(msunt_cd) msunt_cd,\n" + 
				"       min(fcdsc_lower_bound) fcdsc_lower_bound,\n" + 
				"       min(fcdsc_upper_bound) fcdsc_upper_bound\n" + 
				"         for pos in (1 one, 2 two, 3 three)\n" + 
				"      )");
		return RepeatStatus.FINISHED;
	}
}
