# dbascripts
scripts for dba
bigtrx.py is used for getting mysql biggest transactions from start time to end time, order by size ,time, and affected rows.

compare-data-v2 can compre mysql data,for database has 50 tables each table has 5000000 rows, it takes 20 seconds. 

`begin compare
begin compare
Queue.startMonitoring(DEFAULT)table name : sbtest5
table name : user
queue len is 0queue len is 0table compare elapsed: 348.316028ms
begin to find diff
find diffrent rows for table:sbtest5 in chunk_num:0
find diffrent rows id: 2
find diffrent rows for table:user in chunk_num:0
find diffrent rows id: 1
find diffrent rows for table:user in chunk_num:0
find diffrent rows id: 2
find diffrent rows for table:user in chunk_num:0
find diffrent rows id: 3
done`

 for replication that doesn't have too high qps, compare-data-v2 can compare data.
 you can add it to daily task very easy.


