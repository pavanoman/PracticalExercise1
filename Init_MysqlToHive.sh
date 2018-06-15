#!/bin/sh

echo "Creating Hive Database....."

hive -e "CREATE DATABASE hive_practical_exercise_1;"

echo "Importing Data to Hive....."


sqoop import --connect jdbc:mysql://localhost/practical_exercise_1 --username root --password-file /user/cloudera/root_pwd.txt --table user -m 4 --hive-import --hive-database hive_practical_exercise_1 --hive-table user


sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --create hive_practical_exercise_1.activitylog -- import --connect jdbc:mysql://localhost/practical_exercise_1 --username root --password-file /user/cloudera/root_pwd.txt --table activitylog -m 4 --hive-import --hive-database hive_practical_exercise_1 --hive-table activitylog --incremental append --check-column id --last-value 0


sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --exec hive_practical_exercise_1.activitylog


echo "....Data Imported to Hive"

hive -e "SELECT * FROM hive_practical_exercise_1.user;"
hive -e "SELECT * FROM hive_practical_exercise_1.activitylog;"
