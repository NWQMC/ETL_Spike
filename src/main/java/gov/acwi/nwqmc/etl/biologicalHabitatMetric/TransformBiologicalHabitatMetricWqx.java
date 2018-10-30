package gov.acwi.nwqmc.etl.biologicalHabitatMetric;

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
public class TransformBiologicalHabitatMetricWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformBiologicalHabitatMetricWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into bio_hab_metric_swap_storet (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,\n" + 
				"                                   index_identifier, index_type_identifier, index_type_context, index_type_name, resource_title_name,\n" + 
				"                                   resource_creator_name, resource_subject_text, resource_publisher_name, resource_date, resource_identifier,\n" + 
				"                                   index_type_scale_text, index_score_numeric, index_qualifier_code, index_comment, index_calculated_date)\n" + 
				"select /*+ parallel(4) */ \n" + 
				"       3 data_source_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       biological_habitat_index.mloc_uid station_id, \n" + 
				"       station.site_id,\n" + 
				"       station.organization,\n" + 
				"       station.site_type,\n" + 
				"       station.huc,\n" + 
				"       station.governmental_unit_code,\n" + 
				"       biological_habitat_index.bhidx_id index_identifier,\n" + 
				"       index_type.idxtyp_id index_type_identifier,\n" + 
				"       index_type.idxtyp_context index_type_context,\n" + 
				"       index_type.idxtyp_name index_type_name,\n" + 
				"       citation.citatn_title resource_title_name,\n" + 
				"       citation.citatn_creator resource_creator_name,\n" + 
				"       citation.citatn_subject resource_subject_text,\n" + 
				"       citation.citatn_publisher resource_publisher_name,\n" + 
				"       citation.citatn_date resource_date,\n" + 
				"       citation.citatn_id resource_identifier,\n" + 
				"       index_type.idxtyp_scale index_type_scale_text,\n" + 
				"       biological_habitat_index.bhidx_score index_score_numeric,\n" + 
				"       biological_habitat_index.bhidx_qualifier_cd index_qualifier_code,\n" + 
				"       biological_habitat_index.bhidx_comment index_comment,\n" + 
				"       biological_habitat_index.bhidx_calculated_date index_calculated_date\n" + 
				"  from wqx.biological_habitat_index\n" + 
				"       left join station_swap_storet station\n" + 
				"         on biological_habitat_index.mloc_uid = station.station_id\n" + 
				"       left join wqx.index_type\n" + 
				"         on biological_habitat_index.idxtyp_uid = index_type.idxtyp_uid\n" + 
				"       left join wqx.citation\n" + 
				"         on index_type.citatn_uid = citation.citatn_uid");
		return RepeatStatus.FINISHED;
	}
}
