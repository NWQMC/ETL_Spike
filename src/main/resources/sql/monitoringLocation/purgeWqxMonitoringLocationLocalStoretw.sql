delete from wqx_station_local
 where station_source = 'STORETW' and
       not exists (select null
                     from station_no_source
                    where wqx_station_local.station_id = station_no_source.station_id)