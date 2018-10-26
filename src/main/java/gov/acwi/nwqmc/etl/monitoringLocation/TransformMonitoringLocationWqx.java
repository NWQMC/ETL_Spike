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
public class TransformMonitoringLocationWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformMonitoringLocationWqx(JdbcTemplate jdbcTemplate) {
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
				"       monitoring_location.mloc_uid station_id,\n" + 
				"       org.org_id || '-' || monitoring_location.mloc_id site_id,\n" + 
				"       org.org_id organization,\n" + 
				"       wqx_site_type_conversion.station_group_type site_type,\n" + 
				"       nvl(wqx_station_local.calculated_huc_12, nvl(mloc_huc_12, mloc_huc_8)) huc,\n" + 
				"       case\n" + 
				"         when wqx_station_local.calculated_fips is null or\n" + 
				"              substr(wqx_station_local.calculated_fips, 3) = '000'\n" + 
				"           then wqx_station_local.cntry_cd || ':' || wqx_station_local.st_fips_cd || ':' || wqx_station_local.cnty_fips_cd\n" + 
				"         else 'US:' || substr(wqx_station_local.calculated_fips, 1, 2) || ':' || substr(wqx_station_local.calculated_fips, 3, 3)\n" + 
				"       end governmental_unit_code, \n" + 
				"       wqx_station_local.geom,\n" + 
				"       trim(monitoring_location.mloc_name) station_name,\n" + 
				"       org.org_name organization_name,\n" + 
				"       trim(monitoring_location.mloc_desc) description_text,\n" + 
				"       monitoring_location_type.mltyp_name station_type_name,\n" + 
				"       monitoring_location.mloc_latitude latitude,\n" + 
				"       monitoring_location.mloc_longitude longitude,\n" + 
				"       cast(monitoring_location.mloc_source_map_scale as varchar2(4000 char)) map_scale,\n" + 
				"       horizontal_collection_method.hcmth_name geopositioning_method,\n" + 
				"       horizontal_reference_datum.hrdat_name hdatum_id_code,\n" + 
				"       monitoring_location.mloc_vertical_measure elevation_value,\n" + 
				"       nvl2(monitoring_location.mloc_vertical_measure, measurement_unit.msunt_cd, null) elevation_unit,\n" + 
				"       nvl2(monitoring_location.mloc_vertical_measure, vertical_collection_method.vcmth_name, null) elevation_method,\n" + 
				"       nvl2(monitoring_location.mloc_vertical_measure, vertical_reference_datum.vrdat_name, null) vdatum_id_code,\n" + 
				"       monitoring_location.mloc_horizontal_accuracy geoposition_accy_value,\n" + 
				"       hmeasurement_unit.msunt_cd geoposition_accy_unit\n" + 
				"  from wqx.monitoring_location\n" + 
				"       left join wqx_station_local\n" + 
				"         on monitoring_location.mloc_uid = wqx_station_local.station_id and\n" + 
				"            'WQX' = wqx_station_local.station_source\n" + 
				"       left join wqx.vertical_reference_datum\n" + 
				"         on monitoring_location.vrdat_uid = vertical_reference_datum.vrdat_uid\n" + 
				"       left join wqx.vertical_collection_method\n" + 
				"         on monitoring_location.vcmth_uid = vertical_collection_method.vcmth_uid\n" + 
				"       left join wqx.measurement_unit\n" + 
				"         on monitoring_location.msunt_uid_vertical_measure = measurement_unit.msunt_uid\n" + 
				"       left join wqx.measurement_unit hmeasurement_unit\n" + 
				"         on monitoring_location.msunt_uid_horizontal_accuracy = hmeasurement_unit.msunt_uid\n" + 
				"       left join wqx.horizontal_reference_datum\n" + 
				"         on monitoring_location.hrdat_uid = horizontal_reference_datum.hrdat_uid\n" + 
				"       left join wqx.horizontal_collection_method\n" + 
				"         on monitoring_location.hcmth_uid = horizontal_collection_method.hcmth_uid\n" + 
				"       left join wqx.organization org\n" + 
				"         on monitoring_location.org_uid = org.org_uid\n" + 
				"       left join wqx.monitoring_location_type\n" + 
				"         on monitoring_location.mltyp_uid = monitoring_location_type.mltyp_uid\n" + 
				"       left join wqx_site_type_conversion\n" + 
				"         on monitoring_location.mltyp_uid = wqx_site_type_conversion.mltyp_uid\n" + 
				" where org.org_uid not between 2000 and 2999");
		return RepeatStatus.FINISHED;
	}
}
