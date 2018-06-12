#!/bin/sh

echo Creating Table user_report.........

NOW=$(date +"%s")
echo $NOW


hive -e "DROP TABLE IF EXISTS hive_practical_exercise_1.user_report;"

#UPDATE Column query

hive -e "CREATE TABLE hive_practical_exercise_1.user_report as SELECT user.id, a.cnt from(select user_id, count(*) cnt FROM hive_practical_exercise_1.activitylog WHERE type='UPDATE' group by user_id order by user_id)a right outer join hive_practical_exercise_1.user on user.id=a.user_id;"

#INSERT Column query
hive -e "ALTER TABLE hive_practical_exercise_1.user_report ADD COLUMNS (total_inserts int);"

hive -e "INSERT OVERWRITE TABLE hive_practical_exercise_1.user_report SELECT user_report.id, user_report.cnt,b.cnti from (SELECT user_id, count(*) cnti FROM hive_practical_exercise_1.activitylog WHERE type='INSERT' group by user_id order by user_id)b right outer join hive_practical_exercise_1.user_report on user_report.id=b.user_id;"

#DELETE Column query
hive -e "ALTER TABLE hive_practical_exercise_1.user_report ADD COLUMNS (total_deletes int);"

hive -e "INSERT OVERWRITE TABLE hive_practical_exercise_1.user_report SELECT user_report.id, user_report.cnt,user_report.total_inserts,c.cntd from (SELECT user_id, count(*) cntd FROM hive_practical_exercise_1.activitylog WHERE type='DELETE' group by user_id order by user_id)c right outer join hive_practical_exercise_1.user_report on user_report.id=c.user_id;"


#Last Activity Type Query
hive -e "ALTER TABLE hive_practical_exercise_1.user_report ADD COLUMNS (last_activity_type String);"

hive -e "INSERT OVERWRITE TABLE hive_practical_exercise_1.user_report SELECT user_report.id, user_report.cnt,user_report.total_inserts, user_report.total_deletes,e.type from  (SELECT activitylog.user_id, type from (SELECT user_id,MAX(timestamp) ts from hive_practical_exercise_1.activitylog group by user_id) z left outer join hive_practical_exercise_1.activitylog on z.ts= activitylog.timestamp)e       right outer join hive_practical_exercise_1.user_report on user_report.id=e.user_id;"

#Is User Active Query
hive -e "ALTER TABLE hive_practical_exercise_1.user_report ADD COLUMNS (is_active boolean);"

hive -e "INSERT OVERWRITE TABLE hive_practical_exercise_1.user_report select user_report.id, user_report.cnt,user_report.total_inserts, user_report.total_deletes,user_report.last_activity_type, ftab.s from    (SELECT a.user_id, if  (10000000000 - ts < 172800, 1,0) s from (select activitylog.user_id, ts from (SELECT user_id,MAX(timestamp) ts from hive_practical_exercise_1.activitylog group by user_id) z left outer join hive_practical_exercise_1.activitylog on z.ts= activitylog.timestamp) a) ftab     right outer join hive_practical_exercise_1.user_report on user_report.id=ftab.user_id;"


#Upload Count Query
hive -e "ALTER TABLE hive_practical_exercise_1.user_report ADD COLUMNS (upload_count int);"

hive -e "INSERT OVERWRITE TABLE hive_practical_exercise_1.user_report select user_report.id, user_report.cnt,user_report.total_inserts, user_report.total_deletes,user_report.last_activity_type,user_report.is_active, fu.cf from (select user_id, count(*)cf from hive_practical_exercise_1.csv group by user_id order by user_id)fu right outer join hive_practical_exercise_1.user_report on user_report.id=fu.user_id;"

#Display Table 
hive -e "SELECT * FROM hive_practical_exercise_1.user_report;"

echo ......Job done.....






