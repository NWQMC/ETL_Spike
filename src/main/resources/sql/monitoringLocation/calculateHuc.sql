update wqx.monitoring_location_local upd_table
   set calculated_huc_12 = huc12nometa.huc12
  from wqx.monitoring_location_local src_table
       join huc12nometa
         on st_covers(huc12nometa.geometry, src_table.geom)
 where src_table.calculated_huc_12 is null and 
       upd_table.station_id = src_table.station_id and
       upd_table.monitoring_location_source = src_table.monitoring_location_source
