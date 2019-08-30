delete from wqx.monitoring_location_local
 where monitoring_location_source = 'WQX' and
       not exists (select null
                     from wqx_dump."MONITORING_LOCATION" monitoring_location
                          left join wqx_dump."ORGANIZATION" org
                            on monitoring_location."ORG_UID" = org."ORG_UID"
                    where wqx.monitoring_location_local.station_id = monitoring_location."MLOC_UID" and
                          monitoring_location."ORG_UID" not between 2000 and 2999)
