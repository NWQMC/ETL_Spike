insert /*+ append parallel(4) */ into wqx_activity_project (act_uid, project_id_list, project_name_list)
select /*+ parallel(4) */
       activity_project.act_uid,
       listagg(project.prj_id, ';') within group (order by project.prj_id) project_id_list,
       listagg(project.prj_id, ';') within group (order by project.prj_id) project_name_list
--values too long       listagg(project.prj_name, ';') within group (order by project.prj_id) project_name_list
--does not run on wqx        rtrim(clobagg(project.prj_name || '; '), '; ') project_name_list
       from wqx.activity_project
       left join wqx.project
         on activity_project.prj_uid = project.prj_uid
    group by activity_project.act_uid