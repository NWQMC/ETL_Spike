insert
  into wqx.result_lab_sample_prep_sum (res_uid, result_lab_sample_prep_count)
select "RES_UID" res_uid,
       count(*)
  from wqx."RESULT_LAB_SAMPLE_PREP" result_lab_sample_prep
    group by "RES_UID"
