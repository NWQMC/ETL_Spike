insert
  into wqx.result_taxon_habit_aggregated (res_uid, habit_name_list)
select result_taxon_habit."RES_UID" res_uid,
       string_agg(habit."HABIT_NAME", ';' order by habit."HABIT_UID") habit_name_list
  from wqx."RESULT_TAXON_HABIT" result_taxon_habit
       left join wqx."HABIT" habit
         on result_taxon_habit."HABIT_UID" = habit."HABIT_UID"
    group by result_taxon_habit."RES_UID"
