insert
  into act_metric_swap_storet (data_source_id, data_source, station_id, site_id, event_date, activity, sample_media, organization, site_type, huc, governmental_unit_code,
                               organization_name, activity_id, type_identifier, identifier_context, type_name, resource_title, resource_creator, resource_subject, resource_publisher,
                               resource_date, resource_identifier, type_scale, formula_description, measure_value, unit_code, score, comment_text, index_identifier)
select activity.data_source_id,
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
       metric_type."METTYP_ID" type_identifier,
       metric_type_context."MTCTX_CD" identifier_context,
       metric_type."METTYP_NAME" type_name,
       citation."CITATN_TITLE" resource_title,
       citation."CITATN_CREATOR" resource_creator,
       citation."CITATN_SUBJECT" resource_subject,
       citation."CITATN_PUBLISHER" resource_publisher,
       citation."CITATN_DATE" resource_date,
       citation."CITATN_ID" resource_identifier,
       metric_type."METTYP_SCALE" type_scale,
       metric_type."METTYP_FORMULA_DESC" formula_description,
       activity_metric."ACTMET_VALUE" measure_value,
       measurement_unit."MSUNT_CD" unit_code,
       activity_metric."ACTMET_SCORE" score,
       activity_metric."ACTMET_COMMENT" comment_text,
       null index_identifier
  from wqx_dump."ACTIVITY_METRIC" activity_metric
       join activity_swap_storet activity
         on activity_metric."ACT_UID" = activity.activity_id
       left join wqx_dump."METRIC_TYPE" metric_type
         on activity_metric."METTYP_UID" = metric_type."METTYP_UID"
       left join wqx_dump."MEASUREMENT_UNIT" measurement_unit
         on activity_metric."MSUNT_UID_VALUE" = measurement_unit."MSUNT_UID"
       left join wqx_dump."CITATION" citation
         on metric_type."CITATN_UID" = citation."CITATN_UID"
       left join wqx_dump."METRIC_TYPE_CONTEXT" metric_type_context
         on metric_type."MTCTX_UID" = metric_type_context."MTCTX_UID"
