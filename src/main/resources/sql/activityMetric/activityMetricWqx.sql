insert /*+ append parallel(4) */
  into act_metric_swap_storet (data_source_id, data_source, station_id, site_id, event_date, activity, sample_media, organization, site_type, huc, governmental_unit_code,
                               organization_name, activity_id, type_identifier, identifier_context, type_name, resource_title, resource_creator, resource_subject, resource_publisher,
                               resource_date, resource_identifier, type_scale, formula_description, measure_value, unit_code, score, comment_text, index_identifier)
select /*+ parallel(4) */
       activity.data_source_id,
       activity.data_source,
       activity.station_id, 
       activity.site_id,
       activity.event_date,
       activity.activity,
       activity.sample_media,
       activity.organization,
       activity.site_type,
       activity.huc,
       activity.governmental_unit_code,
       activity.organization_name,
       activity.activity_id,
       metric_type.mettyp_id type_identifier,
       metric_type_context.mtctx_cd identifier_context,
       metric_type.mettyp_name type_name,
       citation.citatn_title resource_title,
       citation.citatn_creator resource_creator,
       citation.citatn_subject resource_subject,
       citation.citatn_publisher resource_publisher,
       citation.citatn_date resource_date,
       citation.citatn_id resource_identifier,
       metric_type.mettyp_scale type_scale,
       metric_type.mettyp_formula_desc formula_description,
       activity_metric.actmet_value measure_value,
       measurement_unit.msunt_cd unit_code,
       activity_metric.actmet_score score,
       activity_metric.actmet_comment comment_text,
       null index_identifier
  from wqx.activity_metric
       join activity_swap_storet activity
         on activity_metric.act_uid = activity.activity_id
       left join wqx.metric_type
         on activity_metric.mettyp_uid = metric_type.mettyp_uid
       left join wqx.measurement_unit
         on activity_metric.msunt_uid_value = measurement_unit.msunt_uid
       left join wqx.citation
         on metric_type.citatn_uid = citation.citatn_uid
       left join wqx.metric_type_context
         on metric_type.mtctx_uid = metric_type_context.mtctx_uid