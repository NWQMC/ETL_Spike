create unlogged table if not exists ${WQX_SCHEMA_NAME}.activity_metric_sum
(act_uid                        numeric
,activity_metric_count          numeric
)
with (fillfactor = 100)
