with new_data as (select 'WQX' monitoring_location_source,
                         monitoring_location."MLOC_UID" station_id,
                         org."ORG_ID" || '-' || monitoring_location."MLOC_UID" site_id,
                         monitoring_location."MLOC_LATITUDE" latitude,
                         monitoring_location."MLOC_LONGITUDE" longitude,
                         monitoring_location."HRDAT_UID" hrdat_uid,
                         coalesce("MLOC_HUC_12", "MLOC_HUC_8") huc,
                         coalesce(country.cntry_cd,country_from_state.cntry_cd) cntry_cd,
                         to_char(state.st_fips_cd, 'fm00') st_fips_cd,
                         county.cnty_fips_cd,
                         case
                           when monitoring_location."MLOC_LONGITUDE" is not null and monitoring_location."MLOC_LATITUDE" is not null
                             then st_transform(st_SetSrid(st_MakePoint(monitoring_location."MLOC_LONGITUDE", monitoring_location."MLOC_LATITUDE"), hrdat_to_srid.srid),  4269)
                         end geom
                    from wqx."MONITORING_LOCATION" monitoring_location
                         join wqx.hrdat_to_srid
                           on monitoring_location."HRDAT_UID" = hrdat_to_srid.hrdat_uid
                         left join wqx."ORGANIZATION" org
                           on monitoring_location."ORG_UID" = org."ORG_UID"
                         left join wqx.country
                           on monitoring_location."CNTRY_UID" = country.cntry_uid
                         left join wqx.state
                           on monitoring_location."ST_UID" = state.st_uid
                         left join wqx.county
                           on monitoring_location."CNTY_UID" = county.cnty_uid
                         left join wqx.country country_from_state
                          on state.cntry_uid = country_from_state.cntry_uid
                   where monitoring_location."ORG_UID" not between 2000 and 2999
                 )
insert into wqx.monitoring_location_local (monitoring_location_source, station_id,site_id,latitude,longitude,hrdat_uid,
                                           huc,cntry_cd,st_fips_cd,cnty_fips_cd,geom)
select * from new_data
  on conflict (monitoring_location_source, station_id) do
    update
       set site_id = excluded.site_id,
           latitude = excluded.latitude,
           longitude = excluded.longitude,
           hrdat_uid = excluded.hrdat_uid,
           huc = excluded.huc,
           cntry_cd = excluded.cntry_cd,
           st_fips_cd = excluded.st_fips_cd,
           cnty_fips_cd = excluded.cnty_fips_cd,
           calculated_huc_12 = null,
           calculated_fips = null,
           geom = excluded.geom
     where monitoring_location_local.latitude is distinct from excluded.latitude or
           monitoring_location_local.longitude is distinct from excluded.longitude or
           monitoring_location_local.hrdat_uid is distinct from excluded.hrdat_uid or
           monitoring_location_local.huc is distinct from excluded.huc or
           monitoring_location_local.cntry_cd is distinct from excluded.cntry_cd or
           monitoring_location_local.st_fips_cd is distinct from excluded.st_fips_cd or
           monitoring_location_local.cnty_fips_cd is distinct from excluded.cnty_fips_cd
