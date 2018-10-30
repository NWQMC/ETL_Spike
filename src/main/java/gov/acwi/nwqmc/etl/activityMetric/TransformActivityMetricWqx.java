package gov.acwi.nwqmc.etl.activityMetric;

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
public class TransformActivityMetricWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformActivityMetricWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into act_metric_swap_storet (data_source_id, data_source, station_id, site_id, event_date, activity, sample_media, organization, site_type, huc, governmental_unit_code,\n" + 
				"                               organization_name, activity_id, type_identifier, identifier_context, type_name, resource_title, resource_creator, resource_subject, resource_publisher,\n" + 
				"                               resource_date, resource_identifier, type_scale, formula_description, measure_value, unit_code, score, comment_text, index_identifier)\n" + 
				"select /*+ parallel(4) */ \n" + 
				"       activity.data_source_id,\n" + 
				"       activity.data_source,\n" + 
				"       activity.station_id, \n" + 
				"       activity.site_id,\n" + 
				"       activity.event_date,\n" + 
				"       activity.activity,\n" + 
				"       activity.sample_media,\n" + 
				"       activity.organization,\n" + 
				"       activity.site_type,\n" + 
				"       activity.huc,\n" + 
				"       activity.governmental_unit_code,\n" + 
				"       activity.organization_name,\n" + 
				"       activity.activity_id,\n" + 
				"       metric_type.mettyp_id type_identifier,\n" + 
				"       metric_type_context.mtctx_cd identifier_context,\n" + 
				"       metric_type.mettyp_name type_name,\n" + 
				"       citation.citatn_title resource_title,\n" + 
				"       citation.citatn_creator resource_creator,\n" + 
				"       citation.citatn_subject resource_subject,\n" + 
				"       citation.citatn_publisher resource_publisher,\n" + 
				"       citation.citatn_date resource_date,\n" + 
				"       citation.citatn_id resource_identifier,\n" + 
				"       metric_type.mettyp_scale type_scale,\n" + 
				"       metric_type.mettyp_formula_desc formula_description,\n" + 
				"       activity_metric.actmet_value measure_value,\n" + 
				"       measurement_unit.msunt_cd unit_code,\n" + 
				"       activity_metric.actmet_score score,\n" + 
				"       activity_metric.actmet_comment comment_text,\n" + 
				"       null index_identifier\n" + 
				"  from wqx.activity_metric\n" + 
				"       join activity_swap_storet activity\n" + 
				"         on activity_metric.act_uid = activity.activity_id\n" + 
				"       left join wqx.metric_type\n" + 
				"         on activity_metric.mettyp_uid = metric_type.mettyp_uid\n" + 
				"       left join wqx.measurement_unit\n" + 
				"         on activity_metric.msunt_uid_value = measurement_unit.msunt_uid\n" + 
				"       left join wqx.citation\n" + 
				"         on metric_type.citatn_uid = citation.citatn_uid\n" + 
				"       left join wqx.metric_type_context\n" + 
				"         on metric_type.mtctx_uid = metric_type_context.mtctx_uid");
		return RepeatStatus.FINISHED;
	}
}
