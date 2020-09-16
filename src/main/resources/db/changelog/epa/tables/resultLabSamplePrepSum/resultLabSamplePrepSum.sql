create table if not exists ${WQX_SCHEMA_NAME}.result_lab_sample_prep_sum
(res_uid                        numeric
,result_lab_sample_prep_count   numeric
)
with (fillfactor = 100)
