merge into wqx_station_local o
      using (select /*+ parallel(4) */
                    station_source,
                    station_id,
                    fips_state_code,
                    fips_county_code
               from county_geom,
                    wqx_station_local
              where sdo_contains(county_geom.geom,  wqx_station_local.geom) = 'TRUE' and
                    cntry_cd in ('AS','PR','UM','US', 'VI') and
                    calculated_fips is null) n
   on (o.station_id = n.station_id and
       o.station_source = n.station_source)
when matched then update set calculated_fips = fips_state_code || fips_county_code