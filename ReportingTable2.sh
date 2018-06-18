#!/bin/sh



for (( c=1; c<=5; c++ )) 
do

	hive -e "Insert into hive_practical_exercise_1.user_total select current_timestamp() , a.cnt , CASE when b.cnt2 is not null then a.cnt-b.cnt2 else null end from (select count(DISTINCT id) cnt from hive_practical_exercise_1.user ) as a, (select max(total_users) cnt2 from hive_practical_exercise_1.user_total ) as b ;"

	if [ $? -eq 0 ]; then
	  echo success
          break
	else
	  echo failed to insert
	fi


done
hive -e "SELECT * FROM hive_practical_exercise_1.user_total;"







