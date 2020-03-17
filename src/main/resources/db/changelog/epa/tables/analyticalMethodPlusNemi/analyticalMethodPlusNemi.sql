create unlogged table if not exists ${WQX_SCHEMA_NAME}.analytical_method_plus_nemi
(anlmth_uid                     numeric
,anlmth_id                      character varying(20)
,amctx_cd                       character varying(30)
,anlmth_name                    character varying(120)
,anlmth_url                     character varying(256)
,anlmth_qual_type               character varying(25)
,nemi_url                       text
)
with (fillfactor = 100)
