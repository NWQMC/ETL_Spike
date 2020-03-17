insert
  into wqx.analytical_method_plus_nemi (anlmth_uid, anlmth_id, amctx_cd, anlmth_name, anlmth_url, anlmth_qual_type, nemi_url)
select analytical_method."ANLMTH_UID" anlmth_uid,
       analytical_method."ANLMTH_ID" anlmth_id,
       analytical_method_context."AMCTX_CD" amctx_cd,
       analytical_method."ANLMTH_NAME" anlmth_name,
       analytical_method."ANLMTH_URL" anlmth_url,
       analytical_method."ANLMTH_QUAL_TYPE" anlmth_qual_type,
       nemi_wqp_to_epa_crosswalk.nemi_url
  from wqx_dump."ANALYTICAL_METHOD" analytical_method
       left join wqx_dump."ANALYTICAL_METHOD_CONTEXT" analytical_method_context
         on analytical_method."AMCTX_UID" = analytical_method_context."AMCTX_UID"
       left join wqx.nemi_wqp_to_epa_crosswalk
         on analytical_method_context."AMCTX_CD" = nemi_wqp_to_epa_crosswalk.analytical_procedure_source and
            analytical_method."ANLMTH_ID" = nemi_wqp_to_epa_crosswalk.analytical_procedure_id
