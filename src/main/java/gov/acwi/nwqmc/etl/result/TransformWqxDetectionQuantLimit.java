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
public class TransformWqxDetectionQuantLimit implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformWqxDetectionQuantLimit(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into wqx_detection_quant_limit (res_uid, rdqlmt_measure, msunt_cd, dqltyp_name)\n" + 
				"select /*+ parallel(4) */ res_uid, rdqlmt_measure, msunt_cd, dqltyp_name\n" + 
				"  from (select wqx_r_detect_qnt_lmt.res_uid,\n" + 
				"               wqx_r_detect_qnt_lmt.rdqlmt_measure,\n" + 
				"               wqx_r_detect_qnt_lmt.msunt_cd,\n" + 
				"               wqx_r_detect_qnt_lmt.dqltyp_name,\n" + 
				"               dense_rank() over (partition by wqx_r_detect_qnt_lmt.res_uid order by wqx_dql_hierarchy.hierarchy_value) my_rank,\n" + 
				"               rank()over (partition by wqx_r_detect_qnt_lmt.res_uid, wqx_r_detect_qnt_lmt.dqltyp_uid order by rownum) tie_breaker\n" + 
				"          from wqx_r_detect_qnt_lmt\n" + 
				"               join wqx_dql_hierarchy\n" + 
				"                 on wqx_r_detect_qnt_lmt.dqltyp_uid = wqx_dql_hierarchy.dqltyp_uid\n" + 
				"       )\n" + 
				" where my_rank = 1 and\n" + 
				"       tie_breaker = 1");
		return RepeatStatus.FINISHED;
	}
}
