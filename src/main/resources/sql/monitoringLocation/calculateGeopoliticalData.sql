update wqx.monitoring_location_local upd_table
   set calculated_fips = geoid, cntry_cd = 'US'
  from wqx.monitoring_location_local src_table
       join tl_2019_us_county_geopkg
         on st_covers(tl_2019_us_county_geopkg.wkb_geometry, src_table.geom)
 where src_table.calculated_fips is null and
       upd_table.station_id = src_table.station_id and
       upd_table.monitoring_location_source = src_table.monitoring_location_source
