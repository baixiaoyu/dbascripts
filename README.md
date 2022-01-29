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

    
    backup result table 
  
insert into mysql_backup_info(instance_port,stage,stage_status,stage,message,instance_ip,backup_type,backup_begin_time,backup_end_time," \
              "backup_day,to_hdfs,to_hdfs_time,backup_file_checksum,backup_size,backup_file_name,expired,isdelete,backup_tool,backup_storage_type," \
              "backup_storage_ip,backup_storage_zone)

    storage table

insert into storage(storage_type,storage_type ,storage_param ,storage_ip,storage_zone)

    backup policy table

insert into backup_policy(backup_cron,backup_ip,backup_port,backup_took,backup_type,backup_potention,storage_id)



* myanalyzer.py is a processlist analyzer
    *  used for trouble shooting
    *  give the root cause of why mysql become slow by analyzing show processlist
    *  show bloking information
    

* mystructdiff.py 
    *   can compare table structure differences


