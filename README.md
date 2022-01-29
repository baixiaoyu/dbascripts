##About this project

* common used tools for dba

##tools

* bigtrx.py is a big transaction analyzer
    *  can input a binlog file get big transaction information like time,size,rows
    *  can get big transaction details
    *  just for explicit commit transaction,for implicate commit transactions, xid endtime is same as starttime,so we can't get real time
    *  we can get transaction information from stream like binlog2sql without using mysqlbinlog,it's a better way.  

* MyBackup.py is backup script
    * can backup multi instance on server
    * can execute mysqldump or xtrabackup backup
    * backup keeps on local server and remote storage
    * dbagent monitor policy change and setup/remove backup scripts

* myanalyzer.py is a processlist analyzer
    *  used for trouble shooting
    *  give the root cause of why mysql become slow by analyzing show processlist
    *  show bloking information
    

* mystructdiff.py 
    *   can compare table structure differences


