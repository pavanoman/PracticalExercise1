#!/bin/sh

n=1
#continue for 4 tries 

while [ $n -le 4 ]

do 

	echo Importing Data to Hive....

	hive -e "CREATE DATABASE hive_practical_exercise_1;"
	hive -e "DROP TABLE IF EXISTS hive_practical_exercise_1.user;"
	hive -e "DROP TABLE IF EXISTS hive_practical_exercise_1.activitylog;"

	sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --create hive_practical_exercise_1.user -- import --connect jdbc:mysql://localhost/practical_exercise_1 --username root --password-file /user/cloudera/root_pwd.txt --table user -m 1 --hive-import --hive-database hive_practical_exercise_1 --hive-table user --incremental append --check-column id --last-value 0
          
        sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --exec hive_practical_exercise_1.user



	sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --create hive_practical_exercise_1.activitylog -- import --connect jdbc:mysql://localhost/practical_exercise_1 --username root --password-file /user/cloudera/root_pwd.txt --table activitylog -m 1 --hive-import --hive-database hive_practical_exercise_1 --hive-table activitylog --incremental append --check-column id --last-value 0


  sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --exec hive_practical_exercise_1.activitylog

	hive -e "SELECT * FROM hive_practical_exercise_1.user;"
	hive -e "SELECT * FROM hive_practical_exercise_1.activitylog;"

 

	if [ $? -eq 0 ]
	then
	  echo Successful!
	  exit 0
	  break
        else 
          echo Process failed, Trying again......
	fi

	n=$(( n+1 ))	 # increments $n


done

if [$n == 5]
then
   echo Job Failed
   exit 1
fi

