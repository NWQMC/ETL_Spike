select *
  from (select wqp_source, analytical_procedure_source, analytical_procedure_id,
               source_method_identifier, method_id, method_source, method_type,
               count(*) over (partition by analytical_procedure_source, analytical_procedure_id) cnt
          from wqp_nemi_epa_crosswalk)
 where cnt = 1