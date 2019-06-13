delete from wqx.monitoring_location_local
 where monitoring_location_source = 'STORETW' and
       not exists (select null
                     from storetw.station_no_source
                    where monitoring_location_local.station_id = station_no_source.station_id)