merge into wqx_station_local o 
      using (select /*+ parallel(4) */
                    'WQX' station_source,
                    monitoring_location.mloc_uid station_id,
                    org.org_id || '-' || monitoring_location.mloc_id site_id,
                    monitoring_location.mloc_latitude latitude,
                    monitoring_location.mloc_longitude longitude,
                    monitoring_location.hrdat_uid hrdat_uid,
                    nvl(mloc_huc_12, mloc_huc_8) huc,
                    nvl(country.cntry_cd,country_from_state.cntry_cd) cntry_cd,
                    to_char(state.st_fips_cd, 'fm00') st_fips_cd,
                    county.cnty_fips_cd,
                    sdo_cs.transform(mdsys.sdo_geometry(2001,
                                                        wqx_hrdat_to_srid.srid,
                                                        mdsys.sdo_point_type(round(monitoring_location.mloc_longitude, 7),
                                                                             round(monitoring_location.mloc_latitude, 7),
                                                                             null),
                                                        null, null),
                                     4269) geom
               from wqx.monitoring_location
                    join wqx_hrdat_to_srid
                      on monitoring_location.hrdat_uid = wqx_hrdat_to_srid.hrdat_uid
                    left join wqx.organization org
                      on monitoring_location.org_uid = org.org_uid
                    left join wqx.country
                      on monitoring_location.cntry_uid = country.cntry_uid
                    left join wqx.state
                      on monitoring_location.st_uid = state.st_uid
                    left join wqx.county
                      on monitoring_location.cnty_uid = county.cnty_uid
                    left join wqx.country country_from_state
                      on state.cntry_uid = country_from_state.cntry_uid
              where org.org_uid not between 2000 and 2999
            ) n
  on (o.station_source = n.station_source and
      o.station_id = n.station_id)
when matched then update
                     set o.site_id = n.site_id,
                         o.latitude = n.latitude,
                         o.longitude = n.longitude,
                         o.hrdat_uid = n.hrdat_uid,
                         o.huc = n.huc,
                         o.cntry_cd = n.cntry_cd,
                         o.st_fips_cd = n.st_fips_cd,
                         o.cnty_fips_cd = n.cnty_fips_cd,
                         o.calculated_huc_12 = null,
                         o.calculated_fips = null,
                         o.geom = n.geom
                   where lnnvl(o.latitude = n.latitude) or
                         lnnvl(o.longitude = n.longitude) or
                         lnnvl(o.hrdat_uid = n.hrdat_uid) or
                         lnnvl(o.huc = n.huc) or
                         lnnvl(o.cntry_cd = n.cntry_cd) or
                         lnnvl(o.st_fips_cd = n.st_fips_cd) or
                         lnnvl(o.cnty_fips_cd = n.cnty_fips_cd)
when not matched then insert (station_source, station_id, site_id, latitude, longitude, hrdat_uid, huc, cntry_cd, st_fips_cd, cnty_fips_cd, geom)
                      values (n.station_source, n.station_id, n.site_id, n.latitude, n.longitude, n.hrdat_uid, n.huc, n.cntry_cd, n.st_fips_cd,
                              n.cnty_fips_cd, n.geom)