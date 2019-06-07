insert
  into activity_swap_storet (data_source_id, data_source, station_id, site_id, monitoring_location_name, event_date, activity, sample_media, organization, site_type, huc, governmental_unit_code,
                             organization_name, activity_id, activity_type_code, activity_media_subdiv_name, activity_start_time, act_start_time_zone,
                             activity_stop_date, activity_stop_time, act_stop_time_zone, activity_relative_depth_name, activity_depth,
                             activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit, activity_lower_depth,
                             activity_lower_depth_unit, project_id, project_name, activity_conducting_org, activity_comment, activity_latitude, activity_longitude,
                             activity_source_map_scale, act_horizontal_accuracy, act_horizontal_accuracy_unit, act_horizontal_collect_method,
                             act_horizontal_datum_name, assemblage_sampled_name, act_collection_duration, act_collection_duration_unit,
                             act_sam_compnt_name, act_sam_compnt_place_in_series, act_reach_length, act_reach_length_unit, act_reach_width,
                             act_reach_width_unit, act_pass_count, net_type_name, act_net_surface_area, act_net_surface_area_unit, act_net_mesh_size,
                             act_net_mesh_size_unit, act_boat_speed, act_boat_speed_unit, act_current_speed, act_current_speed_unit,
                             toxicity_test_type_name, sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name,
                             act_sam_collect_meth_qual_type, act_sam_collect_meth_desc, sample_collect_equip_name, act_sam_collect_equip_comments,
                             act_sam_prep_meth_id, act_sam_prep_meth_context, act_sam_prep_meth_name, act_sam_prep_meth_qual_type,
                             act_sam_prep_meth_desc, sample_container_type, sample_container_color, act_sam_chemical_preservative,
                             thermal_preservative_name, act_sam_transport_storage_desc--, activity_object_name, activity_object_type,
--                             activity_file_url, activity_metric_url
							)
select 3 data_source_id,
       'STORET' data_source,
       activity."MLOC_UID" station_id, 
       station.site_id,
       station.station_name,
       date_trunc('day', activity."ACT_START_DATE") event_date,
       station.organization || '-' || activity."ACT_ID" activity,
       activity_media."ACMED_NAME" sample_media,
       station.organization,
       station.site_type,
       station.huc,
       station.governmental_unit_code,
       organization."ORG_NAME" organization_name,
       activity."ACT_UID" activity_id,
       activity_type."ACTYP_CD" activity_type_code,
       activity_media_subdivision."AMSUB_NAME" activity_media_subdiv_name,
       to_char(activity."ACT_START_TIME", 'hh24:mi:ss') activity_start_time,
       start_time_zone."TMZONE_CD" act_start_time_zone,
       to_char(activity."ACT_END_DATE", 'yyyy-mm-dd') activity_stop_date,
	   case
	     when activity."ACT_END_DATE" is not null
		   then to_char(activity."ACT_END_TIME", 'hh24:mi:ss')
	   end activity_stop_time,
       end_time_zone."TMZONE_CD" act_stop_time_zone,
       relative_depth."RELDPTH_NAME" activity_relative_depth_name,
       activity."ACT_DEPTH_HEIGHT" activity_depth,
       h_measurement_unit."MSUNT_CD" activity_depth_unit,
       activity."ACT_DEPTH_ALTITUDE_REF_POINT" activity_depth_ref_point,
       activity."ACT_DEPTH_HEIGHT_TOP" activity_upper_depth,
       t_measurement_unit."MSUNT_CD" activity_upper_depth_unit,
       activity."ACT_DEPTH_HEIGHT_BOTTOM" activity_lower_depth,
       b_measurement_unit."MSUNT_CD" activity_lower_depth_unit,
       activity_project_aggregated.project_id_list project_id,
       activity_project_aggregated.project_name_list project_name,
       activity_conducting_org_aggregated.acorg_name_list activity_conducting_org,
       activity."ACT_COMMENTS" activity_comment,
       coalesce(activity."ACT_LOC_LATITUDE", station.latitude) activity_latitude,
       coalesce(activity."ACT_LOC_LONGITUDE", station.longitude) activity_longitude,
       activity."ACT_LOC_SOURCE_MAP_SCALE" activity_source_map_scale,
       activity."ACT_HORIZONTAL_ACCURACY" act_horizontal_accuracy,
       activity_horizontal_unit."MSUNT_CD" act_horizontal_accuracy_unit,
       horizontal_collection_method."HCMTH_NAME" act_horizontal_collect_method,
       horizontal_reference_datum."HRDAT_NAME" act_horizontal_datum_name,
       assemblage."ASMBLG_NAME" assemblage_sampled_name,
       activity."ACT_COLLECTION_DURATION" act_collection_duration,
       collection_duration."MSUNT_CD" act_collection_duration_unit,
       activity."ACT_SAM_COMPNT_NAME" act_sam_compnt_name,
       activity."ACT_SAM_COMPNT_PLACE_IN_SERIES" act_sam_compnt_place_in_series,
       activity."ACT_REACH_LENGTH" act_reach_length,
       reach_length."MSUNT_CD" act_reach_length_unit,
       activity."ACT_REACH_WIDTH" act_reach_width,
       reach_width."MSUNT_CD" act_reach_width_unit,
       activity."ACT_PASS_COUNT" act_pass_count,
       net_type."NETTYP_NAME" net_type_name,
       activity."ACT_NET_SURFACE_AREA" act_net_surface_area,
       net_surface_unit."MSUNT_CD" act_net_surface_area_unit,
       activity."ACT_NET_MESH_SIZE" act_net_mesh_size,
       net_mesh."MSUNT_CD" act_net_mesh_size_unit,
       activity."ACT_BOAT_SPEED" act_boat_speed,
       boat_speed."MSUNT_CD" act_boat_speed_unit,
       activity."ACT_CURRENT_SPEED" act_current_speed,
       current_speed."MSUNT_CD" act_current_speed_unit,
       toxicity_test_type."TTTYP_NAME" toxicity_test_type_name,
       case
         when activity."ACT_SAM_COLLECT_METH_ID" is not null and
              activity."ACT_SAM_COLLECT_METH_CONTEXT" is not null
           then activity."ACT_SAM_COLLECT_METH_ID"
         else null
       end sample_collect_method_id,
       case
         when activity."ACT_SAM_COLLECT_METH_ID" is not null and
              activity."ACT_SAM_COLLECT_METH_CONTEXT" is not null
           then activity."ACT_SAM_COLLECT_METH_CONTEXT"
         else null
       end sample_collect_method_ctx,
       case
         when activity."ACT_SAM_COLLECT_METH_ID" is not null and
              activity."ACT_SAM_COLLECT_METH_CONTEXT" is not null
           then activity."ACT_SAM_COLLECT_METH_NAME"
         else null
       end sample_collect_method_name,
       activity."ACT_SAM_COLLECT_METH_QUAL_TYPE" act_sam_collect_meth_qual_type,
       activity."ACT_SAM_COLLECT_METH_DESC" act_sam_collect_meth_desc,
       sample_collection_equip."SCEQP_NAME" sample_collect_equip_name,
       activity."ACT_SAM_COLLECT_EQUIP_COMMENTS" act_sam_collect_equip_comments,
       activity."ACT_SAM_PREP_METH_ID" act_sam_prep_meth_id,
       activity."ACT_SAM_PREP_METH_CONTEXT" act_sam_prep_meth_context,
       activity."ACT_SAM_PREP_METH_NAME" act_sam_prep_meth_name,
       activity."ACT_SAM_PREP_METH_QUAL_TYPE" act_sam_prep_meth_qual_type,
       activity."ACT_SAM_PREP_METH_DESC" act_sam_prep_meth_desc,
       container_type."CONTYP_NAME" sample_container_type,
       container_color."CONCOL_NAME" sample_container_color,
       activity."ACT_SAM_CHEMICAL_PRESERVATIVE" act_sam_chemical_preservative,
       thermal_preservative."THPRSV_NAME" thermal_preservative_name,
       activity."ACT_SAM_TRANSPORT_STORAGE_DESC" act_sam_transport_storage_desc--,
--       wqx_attached_object_activity.activity_object_name,
--       wqx_attached_object_activity.activity_object_type,
--       case
--         when wqx_attached_object_activity.ref_uid is null
--           then null
--         else
--           '/organizations/' ||
--               pkg_dynamic_list.url_escape(station.organization, 'true') || '/activities/' ||
--               pkg_dynamic_list.url_escape(station.organization, 'true') || '-' ||
--               pkg_dynamic_list.url_escape(activity.act_id, 'true') || '/files'
--       end activity_file_url,
--       case
--         when wqx_activity_metric_sum.act_uid is null
--           then null
--           else
--             '/activities/' ||
--               pkg_dynamic_list.url_escape(station.organization, 'true') || '-' ||
--               pkg_dynamic_list.url_escape(activity.act_id, 'true') || '/activitymetrics'
--       end activity_metric_url
  from wqx."ACTIVITY" activity
       join station_swap_storet station
         on activity."MLOC_UID" = station.station_id
       left join wqx."SAMPLE_COLLECTION_EQUIP" sample_collection_equip
         on activity."SCEQP_UID" = sample_collection_equip."SCEQP_UID"
       left join wqx.activity_conducting_org_aggregated
         on activity."ACT_UID" = activity_conducting_org_aggregated.act_uid
       left join wqx.activity_project_aggregated
         on activity."ACT_UID" = activity_project_aggregated.act_uid
       left join wqx."MEASUREMENT_UNIT" b_measurement_unit
         on activity."MSUNT_UID_DEPTH_HEIGHT_BOTTOM" = b_measurement_unit."MSUNT_UID"
       left join wqx."MEASUREMENT_UNIT" t_measurement_unit
         on activity."MSUNT_UID_DEPTH_HEIGHT_TOP" = t_measurement_unit."MSUNT_UID"
       left join wqx."MEASUREMENT_UNIT" h_measurement_unit
         on activity."MSUNT_UID_DEPTH_HEIGHT" = h_measurement_unit."MSUNT_UID"
       left join wqx."MEASUREMENT_UNIT" net_surface_unit
         on activity."MSUNT_UID_NET_SURFACE_AREA" = net_surface_unit."MSUNT_UID"
       left join wqx."TIME_ZONE" end_time_zone
         on activity."TMZONE_UID_END_TIME" = end_time_zone."TMZONE_UID"
       left join wqx."TIME_ZONE" start_time_zone
         on activity."TMZONE_UID_START_TIME" = start_time_zone."TMZONE_UID"
       left join wqx."ACTIVITY_MEDIA_SUBDIVISION" activity_media_subdivision
         on activity."AMSUB_UID" = activity_media_subdivision."AMSUB_UID"
       left join wqx."ACTIVITY_TYPE" activity_type
         on activity."ACTYP_UID" = activity_type."ACTYP_UID"
       left join wqx."ORGANIZATION" organization
         on activity."ORG_UID" = organization."ORG_UID"
       left join wqx."ACTIVITY_MEDIA" activity_media
         on activity."ACMED_UID" = activity_media."ACMED_UID"
       left join wqx."MEASUREMENT_UNIT" activity_horizontal_unit
         on activity."MSUNT_UID_HORIZONTAL_ACCURACY" = activity_horizontal_unit."MSUNT_UID"
       left join wqx."HORIZONTAL_COLLECTION_METHOD" horizontal_collection_method
         on activity."HCMTH_UID" = horizontal_collection_method."HCMTH_UID"
       left join wqx."HORIZONTAL_REFERENCE_DATUM" horizontal_reference_datum
         on activity."HRDAT_UID" = horizontal_reference_datum."HRDAT_UID"
       left join wqx."ASSEMBLAGE" assemblage
         on activity."ASMBLG_UID" = assemblage."ASMBLG_UID" 
       left join wqx."MEASUREMENT_UNIT" collection_duration	 
         on activity."MSUNT_UID_COLLECTION_DURATION" = collection_duration."MSUNT_UID"
       left join wqx."MEASUREMENT_UNIT" reach_length
         on activity."MSUNT_UID_REACH_LENGTH" = reach_length."MSUNT_UID"
       left join wqx."MEASUREMENT_UNIT" reach_width
         on activity."MSUNT_UID_REACH_WIDTH" = reach_width."MSUNT_UID"
       left join wqx."NET_TYPE" net_type
         on activity."NETTYP_UID" = net_type."NETTYP_UID"
       left join wqx."MEASUREMENT_UNIT" net_mesh
         on activity."MSUNT_UID_NET_MESH_SIZE" = net_mesh."MSUNT_UID"
       left join wqx."MEASUREMENT_UNIT" boat_speed
         on activity."MSUNT_UID_BOAT_SPEED" = boat_speed."MSUNT_UID"
       left join wqx."MEASUREMENT_UNIT" current_speed
         on activity."MSUNT_UID_CURRENT_SPEED" = current_speed."MSUNT_UID"
       left join wqx."TOXICITY_TEST_TYPE" toxicity_test_type
         on activity."TTTYP_UID" = toxicity_test_type."TTTYP_UID"
       left join wqx."CONTAINER_TYPE" container_type
         on activity."CONTYP_UID" = container_type."CONTYP_UID"
       left join wqx."CONTAINER_COLOR" container_color
         on activity."CONCOL_UID" = container_color."CONCOL_UID"
       left join wqx."THERMAL_PRESERVATIVE" thermal_preservative
         on activity."THPRSV_UID" = thermal_preservative."THPRSV_UID"
       left join wqx."RELATIVE_DEPTH" relative_depth
         on activity."RELDPTH_UID" = relative_depth."RELDPTH_UID"
--       left join wqx_attached_object_activity
--         on activity."ORG_UID" = wqx_attached_object_activity.org_uid and
--            activity."ACT_UID" = wqx_attached_object_activity.ref_uid
       left join wqx.activity_metric_sum
         on activity."ACT_UID" = wqx.activity_metric_sum.act_uid
