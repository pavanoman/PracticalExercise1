# PracticalExercise1

ImportToHive.sh:

This will import mysql tables user and activitylog to hive.
I have used sqoop job in order to use use metastore and reduce the load when performing queries multiple times. 
The program if fails runs again and this will happen for up to 4 times. Exit code 0 or 1 is returned according to success or failure of the program.
Also in place of password field I have used txt file which in hdfs and can only be accessed by (in our case)cloudera user.

csvToHive.sh:

This will upload csv data to hive tables.
The program if fails runs again and this will happen for up to 4 times. Exit code 0 or 1 is returned according to success or failure of the program.

ReportingTable.sh:

User Report table: Initially, I am creating table with two columns - user id and updates. After that i am just altering table to add more columns then i am doing join operation old table and new select query table(eg. DELETE) to combine them. This will give me user id, updates and deletes columns. Same process continues for other columns.

ReportingTable2.sh:

User total table: Here i am using current_timestamp() function to get the time when we ran the query. Then total_user from user table using count(distinct id) (sub query 2). For user added coloumn i am getting total_users from user total table by using max function and subtracting from sub query 2.
