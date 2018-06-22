#!/bin/sh


hadoop fs -put /home/cloudera/Documents/LatestCSV/user_upload_dump* workshop/hive/CSVfiles/LatestCSV

        if [ $? -eq 0 ]; then
	  echo successfully copied user uploads from local_machine to hive folder...
          break;
	else
	  echo failed to copy user uploads from local_machine to hive folder...
          exit 1
	fi

mv /home/cloudera/Documents/LatestCSV/user_upload_dump* /home/cloudera/Documents/CSVDump

        if [ $? -eq 0 ]; then
	  echo successfully moved latest user uploads to CSVDump folder of local_machine...
          break;
	else
	  echo failed to move latest user uploads to CSVDump folder of local_machine...
          exit 1
	fi

hadoop fs -cp workshop/hive/CSVfiles/LatestCSV/user_upload_dump* workshop/hive/CSVfiles/CSVDump

        if [ $? -eq 0 ]; then
	  echo successfully copied files from LatestCSV to CSVDump folder of hive ...
          break;
	else
	  echo failed to copy files from LatestCSV to CSVDump folder of hive...
          exit 1
	fi


for (( c=1; c<=5; c++ )) 
do


	impala-shell -q "LOAD DATA INPATH '/user/cloudera/workshop/hive/CSVfiles/LatestCSV/'  INTO TABLE hive_practical_exercise_1.csv;"

	if [ $? -eq 0 ]; then
	  echo successfully imported data to csv table...
          break
	else
	  echo failed to import data to csv table...
          exit 1
	fi

done


impala-shell -q "SELECT * FROM hive_practical_exercise_1.csv;"
