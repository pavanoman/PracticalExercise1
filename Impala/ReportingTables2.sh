#!/bin/sh



for (( c=1; c<=5; c++ )) 
do

	impala-shell -q "Insert into hive_practical_exercise_1.user_total select unix_timestamp() , a.cnt , CASE when b.cnt2 is not null then a.cnt-b.cnt2 else null end from (select count(DISTINCT id) cnt from hive_practical_exercise_1.user ) as a, (select max(total_users) cnt2 from hive_practical_exercise_1.user_total ) as b ;"

	if [ $? -eq 0 ]; then
	  echo success
          break
	else
	  echo failed to insert
          exit 1
	fi


done
impala-shell -q "SELECT * FROM hive_practical_exercise_1.user_total;"








