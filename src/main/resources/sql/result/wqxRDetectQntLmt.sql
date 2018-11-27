insert /*+ append parallel(4) */
  into wqx_r_detect_qnt_lmt (res_uid, rdqlmt_uid, rdqlmt_measure, msunt_cd, dqltyp_uid, dqltyp_name)
select /*+ parallel(4) */
       result_detect_quant_limit.res_uid,
       result_detect_quant_limit.rdqlmt_uid,
       result_detect_quant_limit.rdqlmt_measure,
       measurement_unit.msunt_cd,
       result_detect_quant_limit.dqltyp_uid,
       detection_quant_limit_type.dqltyp_name
  from wqx.result_detect_quant_limit
       left join wqx.measurement_unit
         on result_detect_quant_limit.msunt_uid = measurement_unit.msunt_uid
       left join wqx.detection_quant_limit_type
         on result_detect_quant_limit.dqltyp_uid = detection_quant_limit_type.dqltyp_uid