insert /*+ append parallel(4) */
  into wqx_result_frequency_class (res_uid,
                                   one_fcdsc_name, one_msunt_cd, one_fcdsc_lower_bound, one_fcdsc_upper_bound,
                                   two_fcdsc_name, two_msunt_cd, two_fcdsc_lower_bound, two_fcdsc_upper_bound,
                                   three_fcdsc_name, three_msunt_cd, three_fcdsc_lower_bound, three_fcdsc_upper_bound)
select /*+ parallel(4) */
       *
  from (select result_frequency_class.res_uid,
               row_number() over (partition by res_uid order by fcdsc_name) pos,
               frequency_class_descriptor.fcdsc_name,
               measurement_unit.msunt_cd,
               result_frequency_class.fcdsc_lower_bound,
               result_frequency_class.fcdsc_upper_bound
          from wqx.result_frequency_class
               left join wqx.frequency_class_descriptor
                 on result_frequency_class.fcdsc_uid = frequency_class_descriptor.fcdsc_uid
               left join wqx.measurement_unit
                 on result_frequency_class.msunt_uid = measurement_unit.msunt_uid
       )
pivot (
       min(fcdsc_name) fcdsc_name,
       min(msunt_cd) msunt_cd,
       min(fcdsc_lower_bound) fcdsc_lower_bound,
       min(fcdsc_upper_bound) fcdsc_upper_bound
         for pos in (1 one, 2 two, 3 three)
      )