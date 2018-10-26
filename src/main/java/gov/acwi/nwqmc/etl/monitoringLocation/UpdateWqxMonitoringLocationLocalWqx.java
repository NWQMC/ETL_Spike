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
public class UpdateWqxMonitoringLocationLocalWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public UpdateWqxMonitoringLocationLocalWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("merge into wqx_station_local o \n" + 
				"      using (select /*+ parallel(4) */ \n" + 
				"                    'WQX' station_source,\n" + 
				"                    monitoring_location.mloc_uid station_id,\n" + 
				"                    org.org_id || '-' || monitoring_location.mloc_id site_id,\n" + 
				"                    monitoring_location.mloc_latitude latitude,\n" + 
				"                    monitoring_location.mloc_longitude longitude,\n" + 
				"                    monitoring_location.hrdat_uid hrdat_uid,\n" + 
				"                    nvl(mloc_huc_12, mloc_huc_8) huc,\n" + 
				"                    nvl(country.cntry_cd,country_from_state.cntry_cd) cntry_cd,\n" + 
				"                    to_char(state.st_fips_cd, 'fm00') st_fips_cd,\n" + 
				"                    county.cnty_fips_cd,\n" + 
				"                    sdo_cs.transform(mdsys.sdo_geometry(2001,\n" + 
				"                                                        wqx_hrdat_to_srid.srid,\n" + 
				"                                                        mdsys.sdo_point_type(round(monitoring_location.mloc_longitude, 7),\n" + 
				"                                                                             round(monitoring_location.mloc_latitude, 7),\n" + 
				"                                                                             null),\n" + 
				"                                                        null, null),\n" + 
				"                                     4269) geom\n" + 
				"               from wqx.monitoring_location\n" + 
				"                    join wqx_hrdat_to_srid\n" + 
				"                      on monitoring_location.hrdat_uid = wqx_hrdat_to_srid.hrdat_uid\n" + 
				"                    left join wqx.organization org\n" + 
				"                      on monitoring_location.org_uid = org.org_uid\n" + 
				"                    left join wqx.country\n" + 
				"                      on monitoring_location.cntry_uid = country.cntry_uid\n" + 
				"                    left join wqx.state\n" + 
				"                      on monitoring_location.st_uid = state.st_uid\n" + 
				"                    left join wqx.county\n" + 
				"                      on monitoring_location.cnty_uid = county.cnty_uid\n" + 
				"                    left join wqx.country country_from_state\n" + 
				"                      on state.cntry_uid = country_from_state.cntry_uid\n" + 
				"              where org.org_uid not between 2000 and 2999\n" + 
				"            ) n\n" + 
				"  on (o.station_source = n.station_source and\n" + 
				"      o.station_id = n.station_id)\n" + 
				"when matched then update\n" + 
				"                     set o.site_id = n.site_id,\n" + 
				"                         o.latitude = n.latitude,\n" + 
				"                         o.longitude = n.longitude,\n" + 
				"                         o.hrdat_uid = n.hrdat_uid,\n" + 
				"                         o.huc = n.huc,\n" + 
				"                         o.cntry_cd = n.cntry_cd,\n" + 
				"                         o.st_fips_cd = n.st_fips_cd,\n" + 
				"                         o.cnty_fips_cd = n.cnty_fips_cd,\n" + 
				"                         o.calculated_huc_12 = null,\n" + 
				"                         o.calculated_fips = null,\n" + 
				"                         o.geom = n.geom\n" + 
				"                   where lnnvl(o.latitude = n.latitude) or\n" + 
				"                         lnnvl(o.longitude = n.longitude) or\n" + 
				"                         lnnvl(o.hrdat_uid = n.hrdat_uid) or\n" + 
				"                         lnnvl(o.huc = n.huc) or\n" + 
				"                         lnnvl(o.cntry_cd = n.cntry_cd) or\n" + 
				"                         lnnvl(o.st_fips_cd = n.st_fips_cd) or\n" + 
				"                         lnnvl(o.cnty_fips_cd = n.cnty_fips_cd)\n" + 
				"when not matched then insert (station_source, station_id, site_id, latitude, longitude, hrdat_uid, huc, cntry_cd, st_fips_cd, cnty_fips_cd, geom)\n" + 
				"                      values (n.station_source, n.station_id, n.site_id, n.latitude, n.longitude, n.hrdat_uid, n.huc, n.cntry_cd, n.st_fips_cd,\n" + 
				"                              n.cnty_fips_cd, n.geom)");
		return RepeatStatus.FINISHED;
	}

}
