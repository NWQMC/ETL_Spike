insert /*+ append parallel(4) */
  into station_swap_storet (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,
                            geom, station_name, organization_name, description_text, station_type_name, latitude, longitude, map_scale,
                            geopositioning_method, hdatum_id_code, elevation_value, elevation_unit, elevation_method, vdatum_id_code,
                            geoposition_accy_value, geoposition_accy_unit
                           )
select /*+ parallel(4) */ 
       3 data_source_id,
       'STORET' data_source,
       wqx_station_local.station_id + 10000000 station_id,
       station_no_source.site_id,
       station_no_source.organization,
       station_no_source.site_type,
       nvl(wqx_station_local.calculated_huc_12, wqx_station_local.huc) huc,
       case
         when wqx_station_local.calculated_fips is null or
              substr(wqx_station_local.calculated_fips, 3) = '000'
           then wqx_station_local.cntry_cd || ':' || wqx_station_local.st_fips_cd || ':' || wqx_station_local.cnty_fips_cd
         else 'US:' || substr(wqx_station_local.calculated_fips, 1, 2) || ':' || substr(wqx_station_local.calculated_fips, 3, 3)
       end governmental_unit_code, 
       wqx_station_local.geom,
       station_no_source.station_name,
       station_no_source.organization_name,
       station_no_source.description_text,
       station_no_source.station_type_name,
       wqx_station_local.latitude,
       wqx_station_local.longitude,
       station_no_source.map_scale,
       station_no_source.geopositioning_method,
       station_no_source.hdatum_id_code,
       station_no_source.elevation_value,
       station_no_source.elevation_unit,
       station_no_source.elevation_method,
       station_no_source.vdatum_id_code,
       null geoposition_accy_value,
       null geoposition_accy_unit
  from wqx_station_local
       join station_no_source
         on wqx_station_local.station_id = station_no_source.station_id
 where wqx_station_local.station_source = 'STORETW'