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
public class CalculateGeopoliticalData implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public CalculateGeopoliticalData(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("merge into wqx_station_local o \n" + 
				"      using (select /*+ parallel(4) */ \n" + 
				"                    station_source,\n" + 
				"                    station_id,\n" + 
				"                    fips_state_code,\n" + 
				"                    fips_county_code\n" + 
				"               from county_geom,\n" + 
				"                    wqx_station_local\n" + 
				"              where sdo_contains(county_geom.geom,  wqx_station_local.geom) = 'TRUE' and\n" + 
				"                    cntry_cd in ('AS','PR','UM','US', 'VI') and\n" + 
				"                    calculated_fips is null) n\n" + 
				"   on (o.station_id = n.station_id and\n" + 
				"       o.station_source = n.station_source)\n" + 
				"when matched then update set calculated_fips = fips_state_code || fips_county_code");
		return RepeatStatus.FINISHED;
	}

}
