#!/bin/sh
echo Importing Data to Hive....

hive -e "CREATE DATABASE hive_practical_exercise_1;"
hive -e "DROP TABLE IF EXISTS hive_practical_exercise_1.user;"
hive -e "DROP TABLE IF EXISTS hive_practical_exercise_1.activitylog;"

sqoop import --connect jdbc:mysql://localhost/practical_exercise_1 --username root --password cloudera --table user -m 1 --hive-import --hive-database hive_practical_exercise_1 --hive-table user 

sqoop import --connect jdbc:mysql://localhost/practical_exercise_1 --username root --password cloudera --table activitylog -m 1 --hive-import --hive-database hive_practical_exercise_1 --hive-table activitylog 

hive -e "SELECT * FROM hive_practical_exercise_1.user;"
hive -e "SELECT * FROM hive_practical_exercise_1.activitylog;"

echo .........Job Done.........
