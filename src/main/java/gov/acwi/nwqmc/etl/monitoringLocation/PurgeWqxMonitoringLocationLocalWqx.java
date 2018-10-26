package gov.acwi.nwqmc.etl.monitoringLocation;

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
public class PurgeWqxMonitoringLocationLocalWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public PurgeWqxMonitoringLocationLocalWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("delete from wqx_station_local\n" + 
				" where station_source = 'WQX' and\n" + 
				"       not exists (select null\n" + 
				"                     from wqx.monitoring_location\n" + 
				"                          left join wqx.organization org\n" + 
				"                            on monitoring_location.org_uid = org.org_uid\n" + 
				"                    where wqx_station_local.station_id = monitoring_location.mloc_uid and\n" + 
				"                          org.org_uid not between 2000 and 2999)");
		return RepeatStatus.FINISHED;
	}

}
