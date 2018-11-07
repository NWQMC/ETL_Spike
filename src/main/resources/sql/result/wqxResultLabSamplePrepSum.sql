insert /*+ append parallel(4) */
  into wqx_result_lab_sample_prep_sum (res_uid, result_lab_sample_prep_count)
select /*+ parallel(4) */
       res_uid,
       count(*)
  from wqx.result_lab_sample_prep
    group by res_uid