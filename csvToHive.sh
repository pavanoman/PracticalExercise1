#!/bin/sh


hadoop fs -put /home/cloudera/Documents/LatestCSV/user_upload_dump* workshop/hive/CSVfiles/LatestCSV

mv /home/cloudera/Documents/LatestCSV/user_upload_dump* /home/cloudera/Documents/CSVDump

hadoop fs -cp workshop/hive/CSVfiles/LatestCSV/user_upload_dump* workshop/hive/CSVfiles/CSVDump


for (( c=1; c<=5; c++ )) 
do


	hive -e "LOAD DATA INPATH 'workshop/hive/CSVfiles/LatestCSV/*' OVERWRITE INTO TABLE hive_practical_exercise_1.csv;"

	if [ $? -eq 0 ]; then
	  echo successfully imported data to csv table...
          break
	else
	  echo failed to import data to csv table...
	fi

done


hive -e "SELECT * FROM hive_practical_exercise_1.csv;"
