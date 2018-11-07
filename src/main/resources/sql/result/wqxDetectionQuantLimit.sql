insert /*+ append parallel(4) */
  into wqx_detection_quant_limit (res_uid, rdqlmt_measure, msunt_cd, dqltyp_name)
select /*+ parallel(4) */ res_uid, rdqlmt_measure, msunt_cd, dqltyp_name
  from (select wqx_r_detect_qnt_lmt.res_uid,
               wqx_r_detect_qnt_lmt.rdqlmt_measure,
               wqx_r_detect_qnt_lmt.msunt_cd,
               wqx_r_detect_qnt_lmt.dqltyp_name,
               dense_rank() over (partition by wqx_r_detect_qnt_lmt.res_uid order by wqx_dql_hierarchy.hierarchy_value) my_rank,
               rank()over (partition by wqx_r_detect_qnt_lmt.res_uid, wqx_r_detect_qnt_lmt.dqltyp_uid order by rownum) tie_breaker
          from wqx_r_detect_qnt_lmt
               join wqx_dql_hierarchy
                 on wqx_r_detect_qnt_lmt.dqltyp_uid = wqx_dql_hierarchy.dqltyp_uid
       )
 where my_rank = 1 and
       tie_breaker = 1