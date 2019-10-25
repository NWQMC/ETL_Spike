with new_data as (select 'STORETW' station_source,
                         station_id,
                         site_id,
                         latitude,
                         longitude,
                         huc,
                         substring(governmental_unit_code, '^[^:]+') cntry_cd,
                         substring(substring(governmental_unit_code, '^[^:]+:[^:]+'), '\d{2}$') st_fips_cd,
                         substring(substring(governmental_unit_code, '^[^:]+:[^:]+:[^:]+$'), '\d{3}$') cnty_fips_cd,
                         geom
                    from storetw.station_no_source
                   where station_no_source.site_id not in (select site_id
                                                             from wqx.monitoring_location_local
                                                            where monitoring_location_source = 'WQX')
            )
insert into wqx.monitoring_location_local (monitoring_location_source, station_id,site_id,latitude,longitude,
                                           huc,cntry_cd,st_fips_cd,cnty_fips_cd,geom)
select * from new_data
  on conflict (monitoring_location_source, station_id) do
    update
       set site_id = excluded.site_id,
           latitude = excluded.latitude,
           longitude = excluded.longitude,
           huc = excluded.huc,
           cntry_cd = excluded.cntry_cd,
           st_fips_cd = excluded.st_fips_cd,
           cnty_fips_cd = excluded.cnty_fips_cd,
           calculated_huc_12 = null,
           calculated_fips = null,
           geom = excluded.geom
     where monitoring_location_local.latitude is distinct from excluded.latitude or
           monitoring_location_local.longitude is distinct from excluded.longitude or
           monitoring_location_local.huc is distinct from excluded.huc or
           monitoring_location_local.cntry_cd is distinct from excluded.cntry_cd or
           monitoring_location_local.st_fips_cd is distinct from excluded.st_fips_cd or
           monitoring_location_local.cnty_fips_cd is distinct from excluded.cnty_fips_cd
