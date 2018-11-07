merge into wqx_station_local o
      using (select /*+ parallel(4) */
                    'STORETW' station_source,
                    station_id,
                    site_id,
                    latitude,
                    longitude,
                    huc,
                    regexp_substr(governmental_unit_code, '[^:]+') cntry_cd,
                    regexp_substr(governmental_unit_code, '[^:]+', 1, 2) st_fips_cd,
                    regexp_substr(governmental_unit_code, '[^:]+', 1, 3) cnty_fips_cd,
                    geom
               from station_no_source
              where station_no_source.site_id not in (select site_id from wqx_station_local where station_source = 'WQX')
            ) n
  on (o.station_source = n.station_source and
      o.station_id = n.station_id)
when matched then update
                     set o.site_id = n.site_id,
                         o.latitude = n.latitude,
                         o.longitude = n.longitude,
                         o.huc = n.huc,
                         o.cntry_cd = n.cntry_cd,
                         o.st_fips_cd = n.st_fips_cd,
                         o.cnty_fips_cd = n.cnty_fips_cd,
                         o.calculated_huc_12 = null,
                         o.calculated_fips = null,
                         o.geom = n.geom
                   where lnnvl(o.latitude = n.latitude) or
                         lnnvl(o.longitude = n.longitude) or
                         lnnvl(o.huc = n.huc) or
                         lnnvl(o.cntry_cd = n.cntry_cd) or
                         lnnvl(o.st_fips_cd = n.st_fips_cd) or
                         lnnvl(o.cnty_fips_cd = n.cnty_fips_cd)
when not matched then insert (station_source, station_id, site_id, latitude, longitude, huc, cntry_cd, st_fips_cd, cnty_fips_cd, geom)
                      values (n.station_source, n.station_id, n.site_id, n.latitude, n.longitude, n.huc, n.cntry_cd, n.st_fips_cd,
                              n.cnty_fips_cd, n.geom)