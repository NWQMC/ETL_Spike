create unlogged table if not exists ${WQX_SCHEMA_NAME}.r_detect_qnt_lmt
(res_uid                        numeric
,rdqlmt_uid                     numeric
,rdqlmt_measure                 character varying(12)
,msunt_cd                       character varying(12)
,dqltyp_uid                     numeric
,dqltyp_name                    character varying(35)
)
with (fillfactor = 100)
