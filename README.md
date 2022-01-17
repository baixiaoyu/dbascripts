##About this project

* common used tools for dba

##tools

* bigtrx.py is a big transaction analyzer
    *  can input a binlog file get big transaction information like time,size,rows
    *  can get big transaction details
    *  just for explicit commit transaction,for implicate commit transactions, xid endtime is same as starttime,so we can't get real time


* myanalyzer.py is a processlist analyzer
    *  used for trouble shooting
    *  give the root cause of why mysql become slow by analyzing show processlist
    *  show bloking information
    

* mystructdiff.py 
    *   can compare table structure differences


