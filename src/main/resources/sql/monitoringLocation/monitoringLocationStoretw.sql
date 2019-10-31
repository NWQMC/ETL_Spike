insert
  into station_swap_storet (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,
                            geom, station_name, organization_name, description_text, station_type_name, latitude, longitude, map_scale,
                            geopositioning_method, hdatum_id_code, elevation_value, elevation_unit, elevation_method, vdatum_id_code
                           )
select 3 data_source_id,
       'STORET' data_source,
       monitoring_location_local.station_id + 10000000 station_id,
       station_no_source.site_id,
       station_no_source.organization,
       station_no_source.site_type,
       coalesce(monitoring_location_local.calculated_huc_12, monitoring_location_local.huc) huc,
       case
         when monitoring_location_local.calculated_fips is null or
              substr(monitoring_location_local.calculated_fips, 3) = '000'
           then coalesce(monitoring_location_local.cntry_cd, '') || ':' || coalesce(monitoring_location_local.st_fips_cd, '') || ':' || coalesce(monitoring_location_local.cnty_fips_cd, '')
         else 'US:' || coalesce(substr(monitoring_location_local.calculated_fips, 1, 2), '') || ':' || coalesce(substr(monitoring_location_local.calculated_fips, 3, 3), '')
       end governmental_unit_code,
       monitoring_location_local.geom,
       station_no_source.station_name,
       station_no_source.organization_name,
       station_no_source.description_text,
       station_no_source.station_type_name,
       monitoring_location_local.latitude,
       monitoring_location_local.longitude,
       station_no_source.map_scale,
       station_no_source.geopositioning_method,
       station_no_source.hdatum_id_code,
       station_no_source.elevation_value,
       station_no_source.elevation_unit,
       station_no_source.elevation_method,
       station_no_source.vdatum_id_code
  from wqx.monitoring_location_local
       join storetw.station_no_source
         on monitoring_location_local.station_id = station_no_source.station_id
 where monitoring_location_local.monitoring_location_source = 'STORETW'