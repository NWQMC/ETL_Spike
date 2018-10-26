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
public class PurgeWqxMonitoringLocationLocalStoretw implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public PurgeWqxMonitoringLocationLocalStoretw(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("delete from wqx_station_local\n" + 
				" where station_source = 'STORETW' and\n" + 
				"       not exists (select null\n" + 
				"                     from station_no_source\n" + 
				"                    where wqx_station_local.station_id = station_no_source.station_id)");
		return RepeatStatus.FINISHED;
	}

}
