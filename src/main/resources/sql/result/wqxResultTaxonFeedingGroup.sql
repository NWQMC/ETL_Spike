insert
  into wqx.result_taxon_feeding_group_aggregated (res_uid, feeding_group_list)
select "RES_UID" res_uid,
       string_agg("RTFGRP_FUNCTIONAL_FEEDING_GRP", ';' order by "RTFGRP_FUNCTIONAL_FEEDING_GRP") feeding_group_list
  from wqx_dump."RESULT_TAXON_FEEDING_GROUP" result_taxon_feeding_group
    group by "RES_UID"
