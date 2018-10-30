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
public class TransformWqpNemiEpaCrosswalk implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformWqpNemiEpaCrosswalk(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
//TODO mock database link - or flip to real batch ItemReader/ItemProcessor/ItemWriter
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into wqp_nemi_epa_crosswalk\n" + 
				"select /*+ parallel(4) */\n" + 
				"       wqp_source,\n" + 
				"       analytical_procedure_source,\n" + 
				"       analytical_procedure_id,\n" + 
				"       source_method_identifier,\n" + 
				"       method_id,\n" + 
				"       method_source,\n" + 
				"       method_type,\n" + 
				"       case\n" + 
				"         when method_id is not null\n" + 
				"           then\n" + 
				"             case method_type\n" + 
				"               when 'analytical'\n" + 
				"                 then 'https://www.nemi.gov/methods/method_summary/' || method_id || '/'\n" + 
				"               when 'statistical'\n" + 
				"                 then 'https://www.nemi.gov/methods/sams_method_summary/' || method_id || '/'\n" + 
				"               end\n" + 
				"         else\n" + 
				"           null\n" + 
				"       end\n" + 
				"  from (select wqp_nemi_epa_crosswalk.*,\n" + 
				"               count(*) over (partition by analytical_procedure_source, analytical_procedure_id) cnt\n" + 
				"          from wqp_nemi_epa_crosswalk@nemi.er.usgs.gov)\n" + 
				" where cnt = 1");
		return RepeatStatus.FINISHED;
	}
}
