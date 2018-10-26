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
public class UpdateWqxMonitoringLocationLocalStoretw implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public UpdateWqxMonitoringLocationLocalStoretw(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("merge into wqx_station_local o \n" + 
				"      using (select /*+ parallel(4) */ \n" + 
				"                    'STORETW' station_source,\n" + 
				"                    station_id,\n" + 
				"                    site_id,\n" + 
				"                    latitude,\n" + 
				"                    longitude,\n" + 
				"                    huc,\n" + 
				"                    regexp_substr(governmental_unit_code, '[^:]+') cntry_cd,\n" + 
				"                    regexp_substr(governmental_unit_code, '[^:]+', 1, 2) st_fips_cd,\n" + 
				"                    regexp_substr(governmental_unit_code, '[^:]+', 1, 3) cnty_fips_cd,\n" + 
				"                    geom\n" + 
				"               from station_no_source\n" + 
				"              where station_no_source.site_id not in (select site_id from wqx_station_local where station_source = 'WQX')\n" + 
				"            ) n\n" + 
				"  on (o.station_source = n.station_source and\n" + 
				"      o.station_id = n.station_id)\n" + 
				"when matched then update\n" + 
				"                     set o.site_id = n.site_id,\n" + 
				"                         o.latitude = n.latitude,\n" + 
				"                         o.longitude = n.longitude,\n" + 
				"                         o.huc = n.huc,\n" + 
				"                         o.cntry_cd = n.cntry_cd,\n" + 
				"                         o.st_fips_cd = n.st_fips_cd,\n" + 
				"                         o.cnty_fips_cd = n.cnty_fips_cd,\n" + 
				"                         o.calculated_huc_12 = null,\n" + 
				"                         o.calculated_fips = null,\n" + 
				"                         o.geom = n.geom\n" + 
				"                   where lnnvl(o.latitude = n.latitude) or\n" + 
				"                         lnnvl(o.longitude = n.longitude) or\n" + 
				"                         lnnvl(o.huc = n.huc) or\n" + 
				"                         lnnvl(o.cntry_cd = n.cntry_cd) or\n" + 
				"                         lnnvl(o.st_fips_cd = n.st_fips_cd) or\n" + 
				"                         lnnvl(o.cnty_fips_cd = n.cnty_fips_cd)\n" + 
				"when not matched then insert (station_source, station_id, site_id, latitude, longitude, huc, cntry_cd, st_fips_cd, cnty_fips_cd, geom)\n" + 
				"                      values (n.station_source, n.station_id, n.site_id, n.latitude, n.longitude, n.huc, n.cntry_cd, n.st_fips_cd,\n" + 
				"                              n.cnty_fips_cd, n.geom)");
		return RepeatStatus.FINISHED;
	}

}
