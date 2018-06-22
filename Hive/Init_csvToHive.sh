#!/bin/sh


hadoop fs -mkdir -p workshop/hive/CSVfiles/CSVDump/
hadoop fs -mkdir -p workshop/hive/CSVfiles/LatestCSV/

for (( c=1; c<=5; c++ )) 
do

	hive -e "CREATE TABLE hive_practical_exercise_1.csv ( user_id int, file_name String, timestamp int) row format delimited fields terminated by ',' stored as textfile tblproperties ('skip.header.line.count'='1');"

	if [ $? -eq 0 ]; then
	  echo successfully created csv table...
          break;
	else
	  echo failed to create csv table...
          exit 1
	fi

done
