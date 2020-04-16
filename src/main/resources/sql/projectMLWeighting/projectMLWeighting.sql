insert
  into prj_ml_weighting_swap_storet (data_source_id,
                                     project_id,
                                     station_id,
                                     data_source,
                                     site_id,
                                     organization,
                                     organization_name,
                                     site_type,
                                     huc,
                                     geom,
                                     governmental_unit_code,
                                     project_identifier,
                                     measure_value,
                                     unit_code,
                                     statistical_stratum,
                                     location_category,
                                     location_status,
                                     ref_location_type_code,
                                     ref_location_start_date,
                                     ref_location_end_date,
                                     resource_title,
                                     resource_creator,
                                     resource_subject,
                                     resource_publisher,
                                     resource_date,
                                     resource_identifier,
                                     comment_text
                                    )
select 3 data_source_id,
       project_data_swap_storet.project_id project_id,
       station_swap_storet.station_id station_id,
       'STORET' data_source,
       station_swap_storet.site_id site_id,
       project_data_swap_storet.organization organization,
       project_data_swap_storet.organization_name organization_name,
       station_swap_storet.site_type site_type,
       station_swap_storet.huc huc,
       station_swap_storet.geom geom,
       station_swap_storet.governmental_unit_code governmental_unit_code,
       project_data_swap_storet.project_identifier project_identifier,
       monitoring_location_weight."MLWT_WEIGHTING_FACTOR" measure_value,
       measurement_unit."MSUNT_CD" unit_code,
       monitoring_location_weight."MLWT_STRATUM" statistical_stratum,
       monitoring_location_weight."MLWT_CATEGORY" location_category,
       monitoring_location_weight."MLWT_STATUS" location_status,
       reference_location_type."RLTYP_CD" ref_location_type_code,
       monitoring_location_weight."MLWT_REF_LOC_START_DATE" ref_location_start_date,
       monitoring_location_weight."MLWT_REF_LOC_END_DATE" ref_location_end_date,
       citation."CITATN_TITLE" resource_title,
       citation."CITATN_CREATOR" resource_creator,
       citation."CITATN_SUBJECT" resource_subject,
       citation."CITATN_PUBLISHER" resource_publisher,
       citation."CITATN_DATE" resource_date,
       citation."CITATN_ID" resource_identifier,
       monitoring_location_weight."MLWT_COMMENT" comment_text
  from wqx_dump."MONITORING_LOCATION_WEIGHT" monitoring_location_weight
       left join wqx_dump."REFERENCE_LOCATION_TYPE" reference_location_type
         on monitoring_location_weight."RLTYP_UID" = reference_location_type."RLTYP_UID"
       left join wqx_dump."MEASUREMENT_UNIT" measurement_unit
         on monitoring_location_weight."MSUNT_UID" = measurement_unit."MSUNT_UID"
       left join wqx_dump."CITATION" citation
         on monitoring_location_weight."CITATN_UID" = citation."CITATN_UID"
       join station_swap_storet
         on monitoring_location_weight."MLOC_UID" = station_swap_storet.station_id
       join project_data_swap_storet
         on monitoring_location_weight."PRJ_UID" = project_data_swap_storet.project_id
