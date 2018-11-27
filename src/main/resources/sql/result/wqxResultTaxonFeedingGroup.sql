insert /*+ append parallel(4) */
  into wqx_result_taxon_feeding_group (res_uid, feeding_group_list)
select /*+ parallel(4) */
       res_uid,
       listagg(rtfgrp_functional_feeding_grp, ';') within group (order by rownum) feeding_group_list
  from wqx.result_taxon_feeding_group
    group by res_uid