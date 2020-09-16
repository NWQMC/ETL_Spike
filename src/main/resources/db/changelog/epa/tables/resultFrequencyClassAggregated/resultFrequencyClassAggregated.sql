create table if not exists ${WQX_SCHEMA_NAME}.result_frequency_class_aggregated
(res_uid                        numeric
,frequency_class_code_1         text
,frequency_class_unit_1         text
,frequency_class_lower_bound_1  text
,frequency_class_upper_bound_1  text
,frequency_class_code_2         text
,frequency_class_unit_2         text
,frequency_class_lower_bound_2  text
,frequency_class_upper_bound_2  text
,frequency_class_code_3         text
,frequency_class_unit_3         text
,frequency_class_lower_bound_3  text
,frequency_class_upper_bound_3  text
)
with (fillfactor = 100)
