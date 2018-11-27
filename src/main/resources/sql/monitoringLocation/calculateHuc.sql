merge into wqx_station_local o
      using (select /*+ parallel(4) */
                    station_source,
                    station_id,
                    huc12
               from huc12nometa,
                    wqx_station_local
              where sdo_contains(huc12nometa.geometry,  wqx_station_local.geom) = 'TRUE' and
                    calculated_huc_12 is null) n
   on (o.station_id = n.station_id and
       o.station_source = n.station_source)
when matched then update set calculated_huc_12 = huc12