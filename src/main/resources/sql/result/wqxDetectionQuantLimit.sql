insert
  into wqx.detection_quant_limit (res_uid, rdqlmt_measure, msunt_cd, dqltyp_name)
select res_uid, rdqlmt_measure, msunt_cd, dqltyp_name
  from (select r_detect_qnt_lmt.res_uid,
               r_detect_qnt_lmt.rdqlmt_measure,
               r_detect_qnt_lmt.msunt_cd,
               r_detect_qnt_lmt.dqltyp_name,
               dense_rank() over (partition by r_detect_qnt_lmt.res_uid order by dql_hierarchy.hierarchy_value) my_rank,
               row_number() over (partition by r_detect_qnt_lmt.res_uid, r_detect_qnt_lmt.dqltyp_uid) tie_breaker
          from wqx.r_detect_qnt_lmt
               join wqx.dql_hierarchy
                 on r_detect_qnt_lmt.dqltyp_uid = dql_hierarchy.dqltyp_uid
       ) x
 where my_rank = 1 and
       tie_breaker = 1