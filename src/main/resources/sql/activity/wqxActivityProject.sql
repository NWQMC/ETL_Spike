insert into wqx.activity_project_aggregated (act_uid, project_id_list, project_name_list)
select activity_project."ACT_UID" act_uid,
       string_agg(project."PRJ_ID", ';' order by project."PRJ_ID") project_id_list,
       string_agg(project."PRJ_NAME", ';' order by project."PRJ_ID") project_name_list
  from wqx_dump."ACTIVITY_PROJECT" activity_project
       left join wqx_dump."PROJECT" project
         on activity_project."PRJ_UID" = project."PRJ_UID"
    group by activity_project."ACT_UID"
