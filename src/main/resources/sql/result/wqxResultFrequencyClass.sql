with result_frequency_classes as (select result_frequency_class."RES_UID" res_uid,
                                          row_number() over (partition by "RES_UID" order by "FCDSC_NAME") pos,
                                          frequency_class_descriptor."FCDSC_NAME" fcdsc_name,
                                          measurement_unit."MSUNT_CD" msunt_cd,
                                          result_frequency_class."FCDSC_LOWER_BOUND" fcdsc_lower_bound,
                                          result_frequency_class."FCDSC_UPPER_BOUND" fcdsc_upper_bound
                                     from wqx."RESULT_FREQUENCY_CLASS" result_frequency_class
                                          left join wqx."FREQUENCY_CLASS_DESCRIPTOR" frequency_class_descriptor
                                            on result_frequency_class."FCDSC_UID" = frequency_class_descriptor."FCDSC_UID"
                                          left join wqx."MEASUREMENT_UNIT" measurement_unit
                                            on result_frequency_class."MSUNT_UID" = measurement_unit."MSUNT_UID")
insert
  into wqx.result_frequency_class_aggregated (res_uid,
                                   frequency_class_code_1, frequency_class_unit_1, frequency_class_lower_bound_1, frequency_class_upper_bound_1,
                                   frequency_class_code_2, frequency_class_unit_2, frequency_class_lower_bound_2, frequency_class_upper_bound_2,
                                   frequency_class_code_3, frequency_class_unit_3, frequency_class_lower_bound_3, frequency_class_upper_bound_3)
select rfc1.res_uid,
        rfc1.fcdsc_name frequency_class_code_1,
        rfc1.msunt_cd frequency_class_unit_1,
        rfc1.fcdsc_lower_bound frequency_class_lower_bound_1,
        rfc1.fcdsc_upper_bound frequency_class_upper_bound_1,
        rfc2.fcdsc_name frequency_class_code_2,
        rfc2.msunt_cd frequency_class_unit_2,
        rfc2.fcdsc_lower_bound frequency_class_lower_bound_2,
        rfc2.fcdsc_upper_bound frequency_class_upper_bound_2,
        rfc3.fcdsc_name frequency_class_code_3,
        rfc3.msunt_cd frequency_class_unit_3,
        rfc3.fcdsc_lower_bound frequency_class_lower_bound_3,
        rfc3.fcdsc_upper_bound frequency_class_upper_bound_3
  from (select *
           from result_frequency_classes
          where pos = 1) rfc1
       left join result_frequency_classes rfc2
         on rfc1.res_uid = rfc2.res_uid and
            2 = rfc2.pos
       left join result_frequency_classes rfc3
         on rfc1.res_uid = rfc3.res_uid and
            3 = rfc3.pos
