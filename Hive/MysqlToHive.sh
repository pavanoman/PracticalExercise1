#!/bin/sh

echo "Importing Data to Hive....."

for (( c=1; c<=5; c++ )) 
do

	sqoop import --connect jdbc:mysql://localhost/practical_exercise_1 --username root --password-file /user/cloudera/root_pwd.txt --table user -m 4 --hive-import --hive-overwrite --hive-database hive_practical_exercise_1 --hive-table user

	if [ $? -eq 0 ]; then
	  echo successfully imported user table...
          break
	else
	  echo failed to import user table...
          exit 1
	fi

done


for (( b=1; b<=5; b++ )) 
do


	sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --exec hive_practical_exercise_1.activitylog

	if [ $? -eq 0 ]; then
	  echo successfully imported activitylog table...
	  break
	else
	  echo failed to import activitylog table...
          exit 1
	fi

done

hive -e "SELECT * FROM hive_practical_exercise_1.user;"

hive -e "SELECT * FROM hive_practical_exercise_1.activitylog;"
