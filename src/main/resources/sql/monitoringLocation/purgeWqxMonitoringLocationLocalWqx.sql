delete from wqx_station_local
 where station_source = 'WQX' and
       not exists (select null
                     from wqx.monitoring_location
                          left join wqx.organization org
                            on monitoring_location.org_uid = org.org_uid
                    where wqx_station_local.station_id = monitoring_location.mloc_uid and
                          org.org_uid not between 2000 and 2999)