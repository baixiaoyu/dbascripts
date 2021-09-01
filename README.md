# dbascripts
scripts for dba
bigtrx.py is used for getting mysql biggest transactions from start time to end time, order by size ,time, and affected rows.

compare-data can compare mysql database data,can get diffrent rows. for database has 50 tables each table has 5000000 rows, it takes 13 minutes, this is first version.

`table compare elapsed: 13m19.442601003s
find diffrent rows for table:sbtest46 in chunk_num:1
find diffrent rows id: 88`

later i'll change some implementation to improve performance.
