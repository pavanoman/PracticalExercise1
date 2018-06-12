#!/bin/sh

NOW=$(date +"%s")

hive -e "DROP TABLE IF EXISTS hive_practical_exercise_1.user_total;"


hive -e "create TABLE hive_practical_exercise_1.user_total(time_ran int, total_users int , users_added int);"

hive -e "Insert into hive_practical_exercise_1.user_total select current_timestamp() , a.cnt , CASE when b.cnt2 is not null then a.cnt-b.cnt2 else null end from (select count(DISTINCT id) cnt from hive_practical_exercise_1.user ) as a, (select max(total_users) cnt2 from hive_practical_exercise_1.user_total ) as b ;"

hive -e "SELECT * FROM hive_practical_exercise_1.user_total;"







