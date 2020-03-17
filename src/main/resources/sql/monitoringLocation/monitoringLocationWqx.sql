insert
  into station_swap_storet (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,
                            geom, station_name, organization_name, description_text, station_type_name, latitude, longitude, map_scale,
                            geopositioning_method, hdatum_id_code, elevation_value, elevation_unit, elevation_method, vdatum_id_code,
                            geoposition_accy_value, geoposition_accy_unit
                           )
select 3 data_source_id,
       'STORET' data_source,
       monitoring_location."MLOC_UID" station_id,
       coalesce(org."ORG_ID", '') || '-' || coalesce(monitoring_location."MLOC_ID", '') site_id,
       org."ORG_ID" organization,
       site_type_conversion.station_group_type site_type,
       coalesce(monitoring_location_local.calculated_huc_12, coalesce("MLOC_HUC_12", "MLOC_HUC_8")) huc,
       case
         when monitoring_location_local.calculated_fips is null or
              substr(monitoring_location_local.calculated_fips, 3) = '000'
           then coalesce(monitoring_location_local.cntry_cd, '') || ':' || coalesce(monitoring_location_local.st_fips_cd, '') || ':' || coalesce(monitoring_location_local.cnty_fips_cd, '')
         else 'US:' || coalesce(substr(monitoring_location_local.calculated_fips, 1, 2), '') || ':' || coalesce(substr(monitoring_location_local.calculated_fips, 3, 3), '')
       end governmental_unit_code, 
       monitoring_location_local.geom,
       trim(monitoring_location."MLOC_NAME") station_name,
       org."ORG_NAME" organization_name,
       trim(monitoring_location."MLOC_DESC") description_text,
       monitoring_location_type."MLTYP_NAME" station_type_name,
       monitoring_location."MLOC_LATITUDE" latitude,
       monitoring_location."MLOC_LONGITUDE" longitude,
       monitoring_location."MLOC_SOURCE_MAP_SCALE"::text map_scale,
       horizontal_collection_method."HCMTH_NAME" geopositioning_method,
       horizontal_reference_datum."HRDAT_NAME" hdatum_id_code,
       monitoring_location."MLOC_VERTICAL_MEASURE" elevation_value,
       case
         when monitoring_location."MLOC_VERTICAL_MEASURE" is not null
           then measurement_unit."MSUNT_CD"
       end elevation_unit,
       case
         when monitoring_location."MLOC_VERTICAL_MEASURE" is not null
           then vertical_collection_method."VCMTH_NAME"
       end elevation_method,
       case
         when monitoring_location."MLOC_VERTICAL_MEASURE" is not null
           then vertical_reference_datum."VRDAT_NAME"
       end vdatum_id_code,
       monitoring_location."MLOC_HORIZONTAL_ACCURACY" geoposition_accy_value,
       hmeasurement_unit."MSUNT_CD" geoposition_accy_unit
  from wqx_dump."MONITORING_LOCATION" monitoring_location
       left join wqx.monitoring_location_local
         on monitoring_location."MLOC_UID" = monitoring_location_local.station_id and
            'WQX' = wqx.monitoring_location_local.monitoring_location_source
       left join wqx_dump."VERTICAL_REFERENCE_DATUM" vertical_reference_datum
         on monitoring_location."VRDAT_UID" = vertical_reference_datum."VRDAT_UID"
       left join wqx_dump."VERTICAL_COLLECTION_METHOD" vertical_collection_method
         on monitoring_location."VCMTH_UID" = vertical_collection_method."VCMTH_UID"
       left join wqx_dump."MEASUREMENT_UNIT" measurement_unit
         on monitoring_location."MSUNT_UID_VERTICAL_MEASURE" = measurement_unit."MSUNT_UID"
       left join wqx_dump."MEASUREMENT_UNIT" hmeasurement_unit
         on monitoring_location."MSUNT_UID_HORIZONTAL_ACCURACY" = hmeasurement_unit."MSUNT_UID"
       left join wqx_dump."HORIZONTAL_REFERENCE_DATUM" horizontal_reference_datum
         on monitoring_location."HRDAT_UID" = horizontal_reference_datum."HRDAT_UID"
       left join wqx_dump."HORIZONTAL_COLLECTION_METHOD" horizontal_collection_method
         on monitoring_location."HCMTH_UID" = horizontal_collection_method."HCMTH_UID"
       left join wqx_dump."ORGANIZATION" org
         on monitoring_location."ORG_UID" = org."ORG_UID"
       left join wqx_dump."MONITORING_LOCATION_TYPE" monitoring_location_type
         on monitoring_location."MLTYP_UID" = monitoring_location_type."MLTYP_UID"
       left join wqx.site_type_conversion
         on monitoring_location."MLTYP_UID" = site_type_conversion.mltyp_uid
 where monitoring_location."ORG_UID" not between 2000 and 2999
