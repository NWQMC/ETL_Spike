insert /*+ append parallel(4) */
  into wqx_analytical_method (anlmth_uid, anlmth_id, amctx_cd, anlmth_name, anlmth_url, anlmth_qual_type, nemi_url)
select /*+ parallel(4) */
       analytical_method.anlmth_uid,
       analytical_method.anlmth_id,
       analytical_method_context.amctx_cd,
       analytical_method.anlmth_name,
       analytical_method.anlmth_url,
       analytical_method.anlmth_qual_type,
       wqp_nemi_epa_crosswalk.nemi_url
  from wqx.analytical_method
       left join wqx.analytical_method_context
         on analytical_method.amctx_uid = analytical_method_context.amctx_uid
       left join wqp_nemi_epa_crosswalk
         on analytical_method_context.amctx_cd = wqp_nemi_epa_crosswalk.analytical_procedure_source and
            analytical_method.anlmth_id = wqp_nemi_epa_crosswalk.analytical_procedure_id