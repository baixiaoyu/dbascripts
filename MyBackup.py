# -*- encoding=utf8 -*-
import datetime
import time
import logging

import re
import os
import requests
import socket

import pymysql 
from os import popen as get


logger = logging.getLogger('MyBackup')
logger.setLevel(level=logging.INFO)

LogFormat = logging.Formatter(
    '%(asctime)s - %(pathname)s - %(filename)s-%(funcName)s-[line:%(lineno)d] - %(levelname)s: %(message)s')

file_handler = logging.FileHandler('/home/mybackup/MyBackup.log')
file_handler.setLevel(level=logging.INFO)
file_handler.setFormatter(LogFormat)
logger.addHandler(file_handler)


def get_local_ip():
    myname = socket.getfqdn(socket.gethostname())
    myaddr = socket.gethostbyname(myname)
    return myaddr


def check_pre_backup_process(port):
    cmd = "ps -ef|grep innobackupex|grep {0}".format(port)
    val = os.popen(cmd)
    ret = val.readlines()
    ret_len =len(ret)
    if ret_len<=1:
        return False
    return True

def check_is_master(port):
    cmd = "mysql -umybackup -pxxx -S /tmp/mysql_{0}.sock -e 'show slave status\G'".format(port)
    result = get(cmd).read().split("\n") 
    slave_status = {}
    for row in result:
        if ":" not in row:
            continue
        k , v = row.split(":" ,1)
        k = k.strip()
        v = v.strip()
        slave_status[k]=v
    if len(slave_status) == 0:
        return True
    if slave_status['Slave_IO_Running'] =="Yes" and slave_status['Slave_SQL_Running'] == "Yes": 
        print('not master')
        return False
    else:
        return True

def check_backuped(port):
    local_ip = get_local_ip()
    today = datetime.date.today()
    sql = " select instance_ip from mysql_backup_info  where backup_day>='{0}' and  instance_port='{1}'".format(today,port) 
    db = DataBase(dbname="cmdb_test", dbhost="xxx", dbuser='dbbackup', dbpwd='dbbackup', dbport=3306)
    backup_ip = db.fetch_all(sql)
    db.close()
    backup_ip_list =[]
    for ip in backup_ip:
        backup_ip_list.append(ip[0])

    if len(backup_ip_list)==0:
        return False
    if local_ip not in backup_ip_list:
        print(local_ip+"not in list")
        return True 
    else:
        print("in list")
        return False 


def send_msg(str):
    post_url = "xxxx"
    head = {"Content-Type": "application/json;" }
    data_json = {"title": "MySQL backup error", "text": str}
    r = requests.post(post_url, data_json, headers=head)


class DataBase:
    def __init__(self, dbname=None, dbhost=None, dbuser=None, dbpwd=None, dbcharset=None, dbport=None):

        self._dbname = dbname
        self._dbhost = dbhost
        self._dbuser = dbuser
        self._dbpassword = dbpwd
        self._dbcharset = dbcharset
        self._dbport = int(dbport)
        self._conn = self.connectMySQL()

        if (self._conn):
            self._cursor = self._conn.cursor()

    # 数据库连接
    def connectMySQL(self):
        conn = False
        try:
            conn = pymysql.connect(host=self._dbhost,
                                   user=self._dbuser,
                                   passwd=self._dbpassword,
                                   db=self._dbname,
                                   port=self._dbport,
                                   charset=self._dbcharset,
                                   )
        except Exception, data:
            logger.error("connect database failed, %s" % data)
            conn = False
        return conn

    # 获取查询结果集
    def fetch_all(self, sql):
        res = ''
        if (self._conn):
            try:
                self._cursor.execute(sql)
                res = self._cursor.fetchall()
            except Exception, data:
                res = False
                logger.error("query database exception, %s" % data)
        return res

    def fetch_one(self, sql):
        rest = ''
        if (self._conn):
            try:
                self._cursor.execute(sql)
                res = self._cursor.fetchone()
            except Exception, data:
                res = False
                logger.error("query database exception, %s" % data)
        return res

    def insert(self, sql):
        flag = False
        if (self._conn):
            try:
                self._cursor.execute(sql)
                self._conn.commit()
                flag = True
            except Exception, data:
                flag = False
                logger.error("insertt table exception, %s" % data)

        return flag

    def update(self, sql):
        flag = False
        if (self._conn):
            try:
                self._cursor.execute(sql)
                self._conn.commit()
                flag = True
            except Exception, data:
                flag = False
                logger.error("update database exception, %s" % data)

        return flag

    def getpkcolumn(self, sql):
        res = ""
        if (self._conn):
            try:
                self._cursor.execute(sql)
                res = self._cursor.fetchone()
            except Exception, data:
                res = False
                logger.error("query database exception, %s" % data)
        return res

    # 关闭数据库连接
    def close(self):
        if (self._conn):
            try:
                if (type(self._cursor) == 'object'):
                    self._cursor.close()
                if (type(self._conn) == 'object'):
                    self._conn.close()
            except Exception, data:
                logger.error("close database exception, %s,%s,%s" % (data, type(self._cursor), type(self._conn)))


class MySqlInstance():

    def get_mysql_port_default_file(self, x):
        '''通过pid获取端口号参数文件位置'''
        port_file = {}
        cmd = 'ps -ef | grep mysqld_safe|grep -v grep'
        try:
            a = os.popen(cmd)
            text = a.readlines()
            for line in text:
                sub_line = line.split('--')[1]
                if sub_line.find("etc")>1:
                    default_file = "/etc/my.cnf"
                    cmd = "cat /etc/my.cnf|grep port|awk -F '=' '{print $2}'" 
                    p = os.popen(cmd)
                    port=p.readlines()[0].strip()
                    port_file[port] = default_file
                else:
                    default_file = sub_line.split('=')[1]
                    data_dir = default_file.split('/')[2]
                    port = data_dir.split('_')[1]
                    port_file[port] = default_file
            return port_file
        except Exception, e:
            logger.error("get mysql info error")
            local_ip = get_local_ip()
            #send_msg("get mysql info error,ip=" + str(local_ip))


class StorageParam():

    def __init__(self):
        self.user = ""
        self.passwd = ""

# 远程备份存放方式
class BackupStorage():
    def __init__(self):
        # 远程的存放方式，hdfs or disk or nfs or aws
        self.storage_type = "hdfs"
        self.storage_dir = ""
        self.storage_param = {}
        self.storage_ip = ""

    def upload_backup(self):
        pass

    def query_backup(self):
        pass

    def delete_backup(self):
        pass

    def create_disk_dir(self,dir_name):
        cmd = "sshpass -p mybackup ssh mybackup@{0}  mkdir -p {1}"
        cmd = cmd.format(self.storage_ip,dir_name)

        try:
            val = os.popen(cmd)
            for i in val.readlines():
                print i
            logger.info("end create remote dir,port:" + str(port))
            return "OK"
        except Exception, e:
            logger.error("create remote dir error")
            return "FAIL"

    def send_to_hdfs(self,file_name):
        logger.info("begin send to hdfs")
        cmd = "hdfs dfs -put " + file_name + self.backup_storage.storage_dir
        print(cmd)
        try:
            val = os.popen(cmd)
            for i in val.readlines():
                print i
            logger.info("end send to hdfs")
            return "OK"

        except Exception, e:
            logger.error(file_name + " send to hdfs error")
            return "FAIL"

class BackupResult():
    def __init__(self):
        self.stage = ""
        self.status = "sucess"
        self.message = "ok"
        self.backup_begin_time=""
        self.backup_end_time=""
        self.backup_file_dir
        self.backup_type= ""
        self.backup_size=""
        self.backup_md5 = ""
        self.port = ""
        self.instance_ip = ""
        self.backup_storage_type = ""
        self.backup_storage_ip = ""
        self.backup_storage_zone = ""

# 备份文件默认就是本地存放一份，然后远程存放一份
class BackupPolicy():
    def __init__(self):
        self.cron = ""
        # mysqldump xtra
        self.backup_tool = "xtra"
        # 全量备份还是增量备份
        self.backup_type = "full"
        # 保留几天
        self.backup_retention = "30"



class InnoBackupEx():
    """Implementation of Backup and Restore for InnoBackupEx."""
    def __int__(self):
        self.__backup_user = 'mybackup'
        self.__backup_user_password = 'qkyo9eGlf0DCGo3x7UHU'
        self.backup_dir = '/data00/backup/'
        self.backup_local_dir = '/data/mybackup/'
        # self.backup_type = "full"
        self.backup_reuslt = BackupResult()
        # 远程存储
        self.backup_storage = BackupStorage()
        self.backup_policy = BackupPolicy()



    def copy_defaults_file(self,file_name,port):
    #     拷贝配置参数文件到备份目录
        pass

    def get_backup_log(self, port):
        backup_pos = self.get_set_today_dir(port)
        backup_log = backup_pos + "/innobackupex.log"
        return backup_log

    def get_socket(self, port):
        socket = "/tmp/mysql_{0}.sock".format(port)
        return socket

    def get_backup_file(self, port):
        today = datetime.date.today()
        now = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        backup_file = self.backup_dir + port + "/" + str(today).replace('-', '') + "/" + self.backup_type + "-" + str(
            now) + ".xbstream"
        return backup_file

    def user_and_pass(self):
        return ('--user=%(user)s --password=%(password)s ' %
                {'user': self.__backup_user,
                 'password': self.__backup_user_password
                 })

    # 设置本地存放的目录
    def get_set_today_dir(self, port):
        today = datetime.date.today()
        backupinfo_dir = self.backup_local_dir + str(today).replace('-', '') + "/" + str(port)
        if not os.path.exists(backupinfo_dir):
            os.makedirs(backupinfo_dir)
        return backupinfo_dir

    def full_or_increment(self, port):
        lsn = None
        if self.backup_policy.backup_tool=="mysqldump":
            self.backup_policy.backup_type == "full"
            return self.backup_policy.backup_type,lsn

        xtra_checkpoints_file = self.get_set_today_dir(port) + "/xtrabackup_checkpoints"
        with open(xtra_checkpoints_file, 'a+') as f:
            f.seek(0)
            for line in f.readlines():
                if "to_lsn" in line:
                    lsn = line.split("=")[1].strip()
                    is_executed = True
                    self.backup_policy.backup_type = "incremental"

        return  self.backup_policy.backup_type , lsn

    def createtargetdir(self, port):

        logger.info("begin to create remote disk dir for port:" + str(port))
        today = datetime.date.today()
        dir_name = self.backup_storage.storage_dir + port + "/" + str(today).replace('-', '')
        res = self.backup_storage.create_disk_dir(dir_name)
        if res == "FAIL":
            self.backup_reuslt.stage = "create remote dir"
            self.backup_reuslt.status="fail"
            self.backup_reuslt.message="fail to create remote disk dir"
            self.save_backup_result(self.backup_reuslt)
            exit(-1)
        else:
            self.backup_reuslt.stage = "create remote dir"
            self.backup_reuslt.status = "success"
            self.backup_reuslt.message = "success to create remote disk dir"
            self.save_backup_result(self.backup_reuslt)

        return res

    def cmd(self, default_file, port):

        if self.backup_storage.storage_type == "disk":
            self.createtargetdir(port)
        else:
            pass
        if self.backup_policy.backup_tool == "mysqldump":
            cmd = "mysqldump  -u {0} -p{1}   --default-character-set=utf8mb4 --set-gtid-purged=off --master-data=2 --single-transaction --all-databases >{2}"
            cmd = cmd.format(self.__backup_user,self.__backup_user_password,self.get_set_today_dir(port))
            return cmd
        else:

            cmd = "innobackupex  --defaults-file={0} --backup  --compress --stream=xbstream --use-memory=2G --extra-lsndir={1} " \
                  "--socket={2} --compress-threads=2 --slave-info --parallel=2 --encrypt=AES256 " \
                  "--encrypt-key=3c0efcea569021b49245e47b5d6a0e28 --encrypt-threads=2 " + self.user_and_pass()


            cmd = cmd.format(default_file, self.get_set_today_dir(port), self.get_socket(port))
            backup_type, lsn = self.full_or_increment(port)
            self.backup_reuslt.backup_filename = self.get_backup_file(port)

            if backup_type and lsn:
                str = "--incremental  --incremental-lsn=" + lsn + " /home/mybackup  2>>" + self.get_backup_log(
                    port)

                return cmd + str
            else:
                str = " /home/mybackup 2>>" + self.get_backup_log(
                    port)
                return cmd +str


    def get_backup_file_size(self, port):
        backup_file = self.backup_reuslt.backup_filename
        cmd = "sshpass -p mybackup ssh mybackup@10.230.38.127 du -sh " + backup_file
        try:
            val = os.popen(cmd)
            for i in val.readlines():
                size = i.split("\t")[0]
            self.backup_reuslt.stage = "get file size"
            self.backup_reuslt.status = "success"
            self.backup_reuslt.message = "success to  get backup file size"
            self.save_backup_result(self.backup_reuslt)
            return size
        except Exception, e:
            logger.error("get backup file size  error")
            self.backup_reuslt.stage = "get file size"
            self.backup_reuslt.status = "fail"
            self.backup_reuslt.message = "fail to  get backup file size"
            self.save_backup_result(self.backup_reuslt)
            exit(-1)

    def get_backup_file_md5(self, port):
        backup_file = self.backup_reuslt.backup_filename
        cmd = "sshpass -p mybackup ssh mybackup@10.230.38.127 md5sum " + backup_file
        try:
            val = os.popen(cmd)
            for i in val.readlines():
                md5 = i.split(" ")[0]
            self.backup_reuslt.stage = "get md5"
            self.backup_reuslt.status = "success"
            self.backup_reuslt.message = "success to  get md5"
            self.save_backup_result(self.backup_reuslt)
            return md5
        except Exception, e:
            logger.error("get backup md5   error")
            self.backup_reuslt.stage = "get md5"
            self.backup_reuslt.status = "fail"
            self.backup_reuslt.message = "fail to  get md5"
            self.save_backup_result(self.backup_reuslt)
            exit(-1)

    def take_backup(self, cmd, port,deault_file):
        logger.info("begin take backup")
        logger.info(cmd)
        self.backup_reuslt.backup_begin_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        try:
            val = os.popen(cmd)
            for i in val.readlines():
                print(i)
            logger.info("end take backup")
            self.backup_reuslt.backup_end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            self.backup_reuslt.status = "take backup"
            self.backup_reuslt.status = "success"
            self.backup_reuslt.message = "success to backup"
            self.save_backup_result(self.backup_reuslt)
        except Exception, e:
            logger.error("take backupe error")
            self.backup_reuslt.stage = "take backup"
            self.backup_reuslt.status = "fail"
            self.backup_reuslt.message = "fail to  send to backup:"+e.message()
            self.save_backup_result(self.backup_reuslt)

            exit(-1)


    def send_backup(self,locak_backup_dir):
        # self.get_backup_file_size(port)
        if self.backup_storage.storage_type == "hdfs":
            res = self.backup_storage.send_to_hdfs(self.backup_reuslt.backup_filename)
            if res == "OK":
                self.backup_reuslt.stage = "send backup to remote"
                self.backup_reuslt.status = "success"
                self.backup_reuslt.message = "success to  send to hdfs"

                self.save_backup_result(self.backup_reuslt)
            else:
                self.backup_reuslt.stage = "send backup to remote"
                self.backup_reuslt.status = "fail"
                self.backup_reuslt.message = "fail to  send to hdfs"

        #         record result
                self.save_backup_result(self.backup_reuslt)
                exit(-1)
        elif self.backup_storage.storage_type == "disk":
            #             拷贝备份到远程disk
            pass

    def remove_expired_bakcup(self,port):
    #    根据port查看备份记录，根据保留策略进行时删除,记录全部在结果表里，即使策略变更过，也能知道之前的备份文件位置，去执行删除
        pass

    def save_backup_result(self,backup_result):
        today = datetime.date.today()
        sql = " insert into mysql_backup_info(instance_port,stage,stage_status,stage,message,instance_ip,backup_type,backup_begin_time,backup_end_time," \
              "backup_day,to_hdfs,to_hdfs_time,backup_file_checksum,backup_size,backup_file_name,expired,isdelete,backup_storage_type," \
              "backup_storage_ip,backup_storage_zone) values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}')".format(
            self.backup_reuslt.port, self.backup_reuslt.stage,self.backup_reuslt.status,self.backup_reuslt.message,self.backup_reuslt.instance_ip,
            self.backup_reuslt.backup_type, self.backup_reuslt.backup_begin_time,
            self.backup_reuslt.backup_end_time, today, 'true', 'null', self.backup_reuslt.backup_md5, self.backup_reuslt.backup_size,
            self.backup_reuslt.backup_file_name,'no','no',backup_result.backup_storage_type,backup_result.backup_storage_ip,
        backup_result.backup_storage_zone)
        try:
            db = DataBase(dbname="cmdb_test", dbhost="xxx", dbuser='dbbackup', dbpwd='dbbackup', dbport=3306)
            db.insert(sql)
        except Exception as  e:
            send_msg("save backup result fail")
        finally:
            db.close()

# 在部署实例的时候，需要在备份策略表中添加一条记录，指定备份的节点，并在元数据上标注，
# dbagent，每分钟执行一下，去数据库中查看策略是否有修改
#每隔1分钟检测下，有对应ip+port 设置的策略就设置定时任务，拉备份脚本，添加备份定时任务，没有对应的ip，就删除定时任务，删除脚本
#dbagent也应该负责check 哪些实例没有添加备份，应该尽可能把定时任务，以及检测任务放到agent上去执行，进行收敛。



if __name__ == '__main__':
    ip = get_local_ip()
    today = datetime.date.today()
    my = MySqlInstance()
    backup = InnoBackupEx()
    port_file_dict = my.get_mysql_port_default_file("mysqld_safe")
    print(port_file_dict)

    for port in port_file_dict:

        backup.backup_reuslt.port=port
        backup.backup_reuslt.instance_ip = ip
        is_master = check_is_master(port)
        backup.remove_expired_bakcup(port)
        if is_master:
            backup.backup_reuslt.status="fail"
            backup.backup_reuslt.message="this is a master,can't do backup job"
            backup.save_backup_result(backup.backup_reuslt)
            continue
        is_backuped = check_backuped(port)
        if is_backuped:
            backup.backup_reuslt.stage="is_backuped"
            backup.backup_reuslt.status = "fail"
            backup.backup_reuslt.message = "this is a master,can't do backup job"
            backup.save_backup_result(backup.backup_reuslt)
            continue
        pre_backup_process = check_pre_backup_process(port) 
        if pre_backup_process:
            backup.backup_reuslt.stage="check_pre_job"
            backup.backup_reuslt.status="fail"
            backup.backup_reuslt.message="pre backup job is executing, can not do it again"
            backup.save_backup_result(backup.backup_reuslt)
            continue


        deault_file = port_file_dict[port]
        cmd = backup.cmd(deault_file, port)
        local_backup_dir = backup.take_backup(cmd, port,deault_file)
        backup.copy_defaults_file(deault_file,port)
        backup.send_backup(local_backup_dir)
        backup.backup_reuslt.backup_size = backup.get_backup_file_size(port)
        backup.backup_reuslt.backup_md5 = backup.get_backup_file_md5(port)
        backup.backup_reuslt.backup_type = backup.backup_policy.backup_type

        if backup.backup_reuslt.backup_size.backup_size == "0":
            send_msg(backup.backup_reuslt.backup_size.backup_file_name+"大小为0，请确认是否正常")
        backup.backup_reuslt.stage="final"
        backup.backup_reuslt.status="sucess"
        backup.save_backup_result(backup.backup_reuslt)


