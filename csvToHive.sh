#!/bin/sh

n=1
#continue for 4 tries 

while [ $n -le 4 ]

do 

	echo Loading CSVs to Hive...........

	hadoop fs -mkdir workshop/
	hadoop fs -mkdir workshop/hive/
	hadoop fs -mkdir workshop/hive/CSVfiles/
	hadoop fs -put csvToUpload.csv workshop/hive/CSVfiles/

	hive -e "DROP TABLE IF EXISTS hive_practical_exercise_1.csv;"

	hive -e "CREATE TABLE hive_practical_exercise_1.csv ( user_id int, file_name String, timestamp int) row format delimited fields terminated by ',' stored as textfile tblproperties ('skip.header.line.count'='1');"

	hive -e "LOAD DATA INPATH 'workshop/hive/CSVfiles/csvToUpload.csv' OVERWRITE INTO TABLE hive_practical_exercise_1.csv;"

	hive -e "SELECT * FROM hive_practical_exercise_1.csv;"


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

