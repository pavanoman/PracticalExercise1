#!/bin/sh



impala-shell -q "CREATE  TABLE hive_practical_exercise_1.updates as SELECT a.user_id, a.cnt from(select user_id, count(*) cnt FROM hive_practical_exercise_1.activitylog WHERE type='UPDATE' group by user_id )a ;"


impala-shell -q "CREATE  TABLE hive_practical_exercise_1.inserts as SELECT b.user_id, b.InsertCount from (SELECT user_id, count(*) InsertCount FROM hive_practical_exercise_1.activitylog WHERE type='INSERT' group by user_id )b ;"


impala-shell -q "CREATE  TABLE hive_practical_exercise_1.deletes as SELECT c.user_id,c.deletecount from (SELECT user_id, count(*) deletecount FROM hive_practical_exercise_1.activitylog WHERE type='DELETE' group by user_id )c ;"


impala-shell -q "CREATE  TABLE hive_practical_exercise_1.temp1 as select activitylog.user_id, ts,type from (SELECT user_id,MAX(timestamps) ts from hive_practical_exercise_1.activitylog group by user_id) z left outer join hive_practical_exercise_1.activitylog on z.ts= activitylog.timestamps ;"

impala-shell -q "CREATE  TABLE hive_practical_exercise_1.activitytype as SELECT user_id, type from  hive_practical_exercise_1.temp1;"


#hive -e "CREATE TEMPORARY TABLE hive_practical_exercise_1.activitytype as SELECT e.user_id, e.type from  (SELECT activitylog.user_id, type from (SELECT user_id,MAX(timestamp) ts from hive_practical_exercise_1.activitylog group by user_id) z left outer join hive_practical_exercise_1.activitylog on z.ts= activitylog.timestamp)e   ;"

impala-shell -q "CREATE  TABLE hive_practical_exercise_1.isactive as  select ftab.user_id, ftab.s from    (SELECT a.user_id, if  ( unix_timestamp() - ts < 172800, 1,0) s from (select user_id, ts from hive_practical_exercise_1.temp1)a )ftab  ;"

#hive -e "CREATE TEMPORARY TABLE hive_practical_exercise_1.isactive as  select ftab.user_id, ftab.s from    (SELECT a.user_id, if  ( unix_timestamp() - ts < 172800, 1,0) s from (select activitylog.user_id, ts from (SELECT user_id,MAX(timestamp) ts from hive_practical_exercise_1.activitylog group by user_id) z left outer join hive_practical_exercise_1.activitylog on z.ts= activitylog.timestamp) a) ftab  ;"


impala-shell -q "CREATE  TABLE hive_practical_exercise_1.uploadcount as  select g.user_id, g.upc from (select user_id, count(*)upc from hive_practical_exercise_1.csv group by user_id )g ;"


impala-shell -q "CREATE TABLE hive_practical_exercise_1.user_report as select u.id ,upd.cnt, ins.InsertCount,del.deletecount,act.type, isa.s, upl.upc
from hive_practical_exercise_1.user u
left outer join hive_practical_exercise_1.updates upd on u.id=upd.user_id
left outer join hive_practical_exercise_1.inserts ins on u.id=ins.user_id
left outer join hive_practical_exercise_1.deletes del on  u.id=del.user_id
left outer join hive_practical_exercise_1.activitytype act on u.id=act.user_id
left outer join hive_practical_exercise_1.isactive isa on u.id=isa.user_id
left outer join hive_practical_exercise_1.uploadcount upl on u.id=upl.user_id;"

impala-shell -q "DROP TABLE hive_practical_exercise_1.updates"
impala-shell -q "DROP TABLE hive_practical_exercise_1.inserts"
impala-shell -q "DROP TABLE hive_practical_exercise_1.deletes"
impala-shell -q "DROP TABLE hive_practical_exercise_1.activitytype"
impala-shell -q "DROP TABLE hive_practical_exercise_1.isactive"
impala-shell -q "DROP TABLE hive_practical_exercise_1.uploadcount"
impala-shell -q "DROP TABLE hive_practical_exercise_1.temp1"
impala-shell -q "select * from hive_practical_exercise_1.user_report;"




