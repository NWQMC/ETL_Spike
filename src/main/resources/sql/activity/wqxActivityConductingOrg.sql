insert  /*+ append parallel(4) */ into wqx_activity_conducting_org (act_uid, acorg_name_list)
select /*+ parallel(4) */
       act_uid,
       listagg(acorg_name, ';') within group (order by rownum) acorg_name_list
  from wqx.activity_conducting_org
    group by act_uid