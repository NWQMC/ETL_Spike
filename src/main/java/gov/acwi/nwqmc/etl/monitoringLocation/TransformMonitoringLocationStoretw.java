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
public class TransformMonitoringLocationStoretw implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformMonitoringLocationStoretw(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into station_swap_storet (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,\n" + 
				"                            geom, station_name, organization_name, description_text, station_type_name, latitude, longitude, map_scale,\n" + 
				"                            geopositioning_method, hdatum_id_code, elevation_value, elevation_unit, elevation_method, vdatum_id_code,\n" + 
				"                            geoposition_accy_value, geoposition_accy_unit\n" + 
				"                           )\n" + 
				"select /*+ parallel(4) */ \n" + 
				"       3 data_source_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       wqx_station_local.station_id + 10000000 station_id,\n" + 
				"       station_no_source.site_id,\n" + 
				"       station_no_source.organization,\n" + 
				"       station_no_source.site_type,\n" + 
				"       nvl(wqx_station_local.calculated_huc_12, wqx_station_local.huc) huc,\n" + 
				"       case\n" + 
				"         when wqx_station_local.calculated_fips is null or\n" + 
				"              substr(wqx_station_local.calculated_fips, 3) = '000'\n" + 
				"           then wqx_station_local.cntry_cd || ':' || wqx_station_local.st_fips_cd || ':' || wqx_station_local.cnty_fips_cd\n" + 
				"         else 'US:' || substr(wqx_station_local.calculated_fips, 1, 2) || ':' || substr(wqx_station_local.calculated_fips, 3, 3)\n" + 
				"       end governmental_unit_code, \n" + 
				"       wqx_station_local.geom,\n" + 
				"       station_no_source.station_name,\n" + 
				"       station_no_source.organization_name,\n" + 
				"       station_no_source.description_text,\n" + 
				"       station_no_source.station_type_name,\n" + 
				"       wqx_station_local.latitude,\n" + 
				"       wqx_station_local.longitude,\n" + 
				"       station_no_source.map_scale,\n" + 
				"       station_no_source.geopositioning_method,\n" + 
				"       station_no_source.hdatum_id_code,\n" + 
				"       station_no_source.elevation_value,\n" + 
				"       station_no_source.elevation_unit,\n" + 
				"       station_no_source.elevation_method,\n" + 
				"       station_no_source.vdatum_id_code,\n" + 
				"       null geoposition_accy_value,\n" + 
				"       null geoposition_accy_unit\n" + 
				"  from wqx_station_local\n" + 
				"       join station_no_source\n" + 
				"         on wqx_station_local.station_id = station_no_source.station_id\n" + 
				" where wqx_station_local.station_source = 'STORETW'");
		return RepeatStatus.FINISHED;
	}
}
