update wqx.monitoring_location_local upd_table
   set calculated_fips = coalesce(fips_state_code, '') || coalesce(fips_county_code, '')
  from wqx.monitoring_location_local src_table
       join county_geom
         on st_covers(county_geom.geom, st_transform(src_table.geom, 4326))
 where src_table.cntry_cd in ('AS','PR','UM','US', 'VI') and
       src_table.calculated_fips is null and 
       upd_table.station_id = src_table.station_id and
       upd_table.monitoring_location_source = src_table.monitoring_location_source
