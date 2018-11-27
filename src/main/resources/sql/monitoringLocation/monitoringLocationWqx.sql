insert /*+ append parallel(4) */
  into station_swap_storet (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,
                            geom, station_name, organization_name, description_text, station_type_name, latitude, longitude, map_scale,
                            geopositioning_method, hdatum_id_code, elevation_value, elevation_unit, elevation_method, vdatum_id_code,
                            geoposition_accy_value, geoposition_accy_unit
                           )
select /*+ parallel(4) */
       3 data_source_id,
       'STORET' data_source,
       monitoring_location.mloc_uid station_id,
       org.org_id || '-' || monitoring_location.mloc_id site_id,
       org.org_id organization,
       wqx_site_type_conversion.station_group_type site_type,
       nvl(wqx_station_local.calculated_huc_12, nvl(mloc_huc_12, mloc_huc_8)) huc,
       case
         when wqx_station_local.calculated_fips is null or
              substr(wqx_station_local.calculated_fips, 3) = '000'
           then wqx_station_local.cntry_cd || ':' || wqx_station_local.st_fips_cd || ':' || wqx_station_local.cnty_fips_cd
         else 'US:' || substr(wqx_station_local.calculated_fips, 1, 2) || ':' || substr(wqx_station_local.calculated_fips, 3, 3)
       end governmental_unit_code, 
       wqx_station_local.geom,
       trim(monitoring_location.mloc_name) station_name,
       org.org_name organization_name,
       trim(monitoring_location.mloc_desc) description_text,
       monitoring_location_type.mltyp_name station_type_name,
       monitoring_location.mloc_latitude latitude,
       monitoring_location.mloc_longitude longitude,
       cast(monitoring_location.mloc_source_map_scale as varchar2(4000 char)) map_scale,
       horizontal_collection_method.hcmth_name geopositioning_method,
       horizontal_reference_datum.hrdat_name hdatum_id_code,
       monitoring_location.mloc_vertical_measure elevation_value,
       nvl2(monitoring_location.mloc_vertical_measure, measurement_unit.msunt_cd, null) elevation_unit,
       nvl2(monitoring_location.mloc_vertical_measure, vertical_collection_method.vcmth_name, null) elevation_method,
       nvl2(monitoring_location.mloc_vertical_measure, vertical_reference_datum.vrdat_name, null) vdatum_id_code,
       monitoring_location.mloc_horizontal_accuracy geoposition_accy_value,
       hmeasurement_unit.msunt_cd geoposition_accy_unit
  from wqx.monitoring_location
       left join wqx_station_local
         on monitoring_location.mloc_uid = wqx_station_local.station_id and
            'WQX' = wqx_station_local.station_source
       left join wqx.vertical_reference_datum
         on monitoring_location.vrdat_uid = vertical_reference_datum.vrdat_uid
       left join wqx.vertical_collection_method
         on monitoring_location.vcmth_uid = vertical_collection_method.vcmth_uid
       left join wqx.measurement_unit
         on monitoring_location.msunt_uid_vertical_measure = measurement_unit.msunt_uid
       left join wqx.measurement_unit hmeasurement_unit
         on monitoring_location.msunt_uid_horizontal_accuracy = hmeasurement_unit.msunt_uid
       left join wqx.horizontal_reference_datum
         on monitoring_location.hrdat_uid = horizontal_reference_datum.hrdat_uid
       left join wqx.horizontal_collection_method
         on monitoring_location.hcmth_uid = horizontal_collection_method.hcmth_uid
       left join wqx.organization org
         on monitoring_location.org_uid = org.org_uid
       left join wqx.monitoring_location_type
         on monitoring_location.mltyp_uid = monitoring_location_type.mltyp_uid
       left join wqx_site_type_conversion
         on monitoring_location.mltyp_uid = wqx_site_type_conversion.mltyp_uid
 where org.org_uid not between 2000 and 2999