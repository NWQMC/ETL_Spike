package gov.acwi.nwqmc.etl.projectMlWeighting;

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
public class TransformProjectMlWeightingWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformProjectMlWeightingWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into prj_ml_weighting_swap_storet (data_source_id,\n" + 
				"                                     project_id,\n" + 
				"                                     station_id,\n" + 
				"                                     data_source,\n" + 
				"                                     site_id,\n" + 
				"                                     organization,\n" + 
				"                                     organization_name,\n" + 
				"                                     site_type,\n" + 
				"                                     huc,\n" + 
				"                                     governmental_unit_code,\n" + 
				"                                     project_identifier,\n" + 
				"                                     measure_value,\n" + 
				"                                     unit_code,\n" + 
				"                                     statistical_stratum,\n" + 
				"                                     location_category,\n" + 
				"                                     location_status,\n" + 
				"                                     ref_location_type_code,\n" + 
				"                                     ref_location_start_date,\n" + 
				"                                     ref_location_end_date,\n" + 
				"                                     resource_title,\n" + 
				"                                     resource_creator,\n" + 
				"                                     resource_subject,\n" + 
				"                                     resource_publisher,\n" + 
				"                                     resource_date,\n" + 
				"                                     resource_identifier,\n" + 
				"                                     comment_text\n" + 
				"                                    )\n" + 
				"select  3 data_source_id,\n" + 
				"        project_data_swap_storet.project_id project_id,\n" + 
				"        station_swap_storet.station_id station_id,\n" + 
				"        'STORET' data_source,\n" + 
				"        station_swap_storet.site_id site_id,\n" + 
				"        project_data_swap_storet.organization organization,\n" + 
				"        project_data_swap_storet.organization_name organization_name,\n" + 
				"        station_swap_storet.site_type site_type,\n" + 
				"        station_swap_storet.huc huc,\n" + 
				"        station_swap_storet.governmental_unit_code governmental_unit_code,\n" + 
				"        project_data_swap_storet.project_identifier project_identifier,\n" + 
				"        monitoring_location_weight.mlwt_weighting_factor measure_value,\n" + 
				"        measurement_unit.msunt_cd unit_code,\n" + 
				"        monitoring_location_weight.mlwt_stratum statistical_stratum,\n" + 
				"        monitoring_location_weight.mlwt_category location_category,\n" + 
				"        monitoring_location_weight.mlwt_status location_status,\n" + 
				"        reference_location_type.rltyp_cd ref_location_type_code,\n" + 
				"        monitoring_location_weight.mlwt_ref_loc_start_date ref_location_start_date,\n" + 
				"        monitoring_location_weight.mlwt_ref_loc_end_date ref_location_end_date,\n" + 
				"        citation.citatn_title resource_title,\n" + 
				"        citation.citatn_creator resource_creator,\n" + 
				"        citation.citatn_subject resource_subject,\n" + 
				"        citation.citatn_publisher resource_publisher,\n" + 
				"        citation.citatn_date resource_date,\n" + 
				"        citation.citatn_id resource_identifier,\n" + 
				"        monitoring_location_weight.mlwt_comment comment_text\n" + 
				"  from wqx.monitoring_location_weight\n" + 
				"        left join wqx.reference_location_type\n" + 
				"          on monitoring_location_weight.rltyp_uid = reference_location_type.rltyp_uid\n" + 
				"        join wqx.measurement_unit\n" + 
				"          on monitoring_location_weight.msunt_uid = measurement_unit.msunt_uid\n" + 
				"        join wqx.citation\n" + 
				"          on monitoring_location_weight.citatn_uid = citation.citatn_uid\n" + 
				"        join wqp_core.station_swap_storet\n" + 
				"          on monitoring_location_weight.mloc_uid = station_swap_storet.station_id\n" + 
				"        join wqp_core.project_data_swap_storet\n" + 
				"          on monitoring_location_weight.prj_uid = project_data_swap_storet.project_id");
		return RepeatStatus.FINISHED;
	}
}
