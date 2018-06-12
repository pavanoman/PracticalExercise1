#!/bin/sh

echo Loading CSVs to Hive...........

hadoop fs -mkdir workshop/
hadoop fs -mkdir workshop/hive/
hadoop fs -mkdir workshop/hive/CSVfiles/
hadoop fs -put csvToUpload2.csv workshop/hive/CSVfiles/

hive -e "DROP TABLE IF EXISTS hive_practical_exercise_1.csv;"

hive -e "CREATE TABLE hive_practical_exercise_1.csv ( user_id int, file_name String, timestamp int) row format delimited fields terminated by ',' stored as textfile;"

hive -e "LOAD DATA INPATH 'workshop/hive/CSVfiles/csvToUpload2.csv' OVERWRITE INTO TABLE hive_practical_exercise_1.csv;"

hive -e "SELECT * FROM hive_practical_exercise_1.csv;"

echo .....Job Done........