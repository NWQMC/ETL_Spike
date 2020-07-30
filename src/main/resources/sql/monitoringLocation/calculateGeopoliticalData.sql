update wqx.monitoring_location_local upd_table
   set calculated_fips = geoid
  from wqx.monitoring_location_local src_table
       join tl_2019_us_county_geopkg
         on st_covers(tl_2019_us_county_geopkg.wkb_geometry, src_table.geom)
 where src_table.cntry_cd in ('AS','PR','UM','US', 'VI') and
       src_table.calculated_fips is null and 
       upd_table.station_id = src_table.station_id and
       upd_table.monitoring_location_source = src_table.monitoring_location_source
