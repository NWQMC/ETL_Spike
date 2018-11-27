insert /*+ append parallel(4) */
  into station_object_swap_storet (data_source_id,
                                   object_id,
                                   data_source,
                                   organization,
                                   station_id,
                                   site_id,
                                   object_name,
                                   object_type,
                                   object_content)
select '3' data_source_id,
       attached_object.atobj_uid object_id,
       'STORET' data_source,
       organization.org_id organization,
       attached_object.ref_uid station_id,
       station.site_id,
       attached_object.atobj_file_name object_name,
       attached_object.atobj_type object_type,
       attached_object.atobj_content object_content
  from wqx.attached_object
       join wqx.organization
         on attached_object.org_uid = organization.org_uid
       join station_swap_storet station
         on attached_object.ref_uid = station.station_id
 where tbl_uid = 2