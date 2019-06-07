insert
  into wqx.r_detect_qnt_lmt (res_uid, rdqlmt_uid, rdqlmt_measure, msunt_cd, dqltyp_uid, dqltyp_name)
select result_detect_quant_limit."RES_UID" res_uid,
       result_detect_quant_limit."RDQLMT_UID" rdqlmt_uid,
       result_detect_quant_limit."RDQLMT_MEASURE" rdqlmt_measure,
       measurement_unit."MSUNT_CD" msunt_cd,
       result_detect_quant_limit."DQLTYP_UID" dqltyp_uid,
       detection_quant_limit_type."DQLTYP_NAME" dqltyp_name
  from wqx."RESULT_DETECT_QUANT_LIMIT" result_detect_quant_limit
       left join wqx."MEASUREMENT_UNIT" measurement_unit
         on result_detect_quant_limit."MSUNT_UID" = measurement_unit."MSUNT_UID"
       left join wqx."DETECTION_QUANT_LIMIT_TYPE" detection_quant_limit_type
         on result_detect_quant_limit."DQLTYP_UID" = detection_quant_limit_type."DQLTYP_UID"
