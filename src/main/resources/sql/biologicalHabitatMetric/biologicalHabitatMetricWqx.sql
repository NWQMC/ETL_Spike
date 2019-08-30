insert 
  into bio_hab_metric_swap_storet (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,
                                   index_identifier, index_type_identifier, index_type_context, index_type_name, resource_title_name,
                                   resource_creator_name, resource_subject_text, resource_publisher_name, resource_date, resource_identifier,
                                   index_type_scale_text, index_score_numeric, index_qualifier_code, index_comment, index_calculated_date)
select 3 data_source_id,
       'STORET' data_source,
       biological_habitat_index."MLOC_UID" station_id,
       station.site_id,
       station.organization,
       station.site_type,
       station.huc,
       station.governmental_unit_code,
       biological_habitat_index."BHIDX_ID" index_identifier,
       index_type."IDXTYP_ID" index_type_identifier,
       index_type."IDXTYP_CONTEXT" index_type_context,
       index_type."IDXTYP_NAME" index_type_name,
       citation."CITATN_TITLE" resource_title_name,
       citation."CITATN_CREATOR" resource_creator_name,
       citation."CITATN_SUBJECT" resource_subject_text,
       citation."CITATN_PUBLISHER" resource_publisher_name,
       citation."CITATN_DATE" resource_date,
       citation."CITATN_ID" resource_identifier,
       index_type."IDXTYP_SCALE" index_type_scale_text,
       biological_habitat_index."BHIDX_SCORE" index_score_numeric,
       biological_habitat_index."BHIDX_QUALIFIER_CD" index_qualifier_code,
       biological_habitat_index."BHIDX_COMMENT" index_comment,
       biological_habitat_index."BHIDX_CALCULATED_DATE" index_calculated_date
  from wqx_dump."BIOLOGICAL_HABITAT_INDEX" biological_habitat_index
       left join station_swap_storet station
         on biological_habitat_index."MLOC_UID" = station.station_id
       left join wqx_dump."INDEX_TYPE" index_type
         on biological_habitat_index."IDXTYP_UID" = index_type."IDXTYP_UID"
       left join wqx_dump."CITATION" citation
         on index_type."CITATN_UID" = citation."CITATN_UID"
