insert into wqx.activity_conducting_org_aggregated (act_uid, acorg_name_list)
select "ACT_UID" act_uid,
       string_agg("ACORG_NAME", ';' order by "ORG_UID") acorg_name_list
  from wqx."ACTIVITY_CONDUCTING_ORG" activity_conducting_org
    group by "ACT_UID"
