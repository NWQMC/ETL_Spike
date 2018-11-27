insert /*+ append parallel(4) */
  into wqx_result_taxon_habit (res_uid, habit_name_list)
select /*+ parallel(4) */
       result_taxon_habit.res_uid,
       listagg(habit.habit_name, ';') within group (order by habit.habit_uid) habit_name_list
  from wqx.result_taxon_habit
       left join wqx.habit
         on result_taxon_habit.habit_uid = habit.habit_uid
    group by result_taxon_habit.res_uid