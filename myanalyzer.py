#!/usr/bin/env python
# coding: utf-8
import argparse
import traceback
from urllib import parse
import os
import sys
from wasabi import Printer, table
import configparser

try:
    import pymysql  # MySQL-python
except ImportError:
    try:
        import pymysql as pymysql  # PyMySQL
    except ImportError:
        print("Please install MySQLdb or PyMySQL")
        sys.exit(1)
from warnings import filterwarnings
filterwarnings('ignore',category=pymysql.Warning)

msg = Printer(line_max=2000)
BIG_TRANSACTION_TIME = 1
SELECT_SHOW_LIMIT = 5
DML_SHOW_LIMIT = 5
DDL_SHOW_LIMIT = 5


def connect(conf='~/.my.cnf', section='DEFAULT', host_ip="", port=""):
    """
    connect to MySQL from conf file.
    """
    try:
        parser = configparser.ConfigParser(allow_no_value=True)
        parser.read([os.path.expanduser(conf)])

        user = parser.get(section, 'user')
        user = user.strip()
        password = parser.get(section, 'password')
        if port == "":
            port = int(parser.get(section, 'port'))
        else:
            port = int(port)
        password = password.strip("\"")
        password = parse.unquote_plus(password)

        if parser.has_option(section, 'socket'):
            socket = parser.get(section, 'socket')
            return pymysql.connect(unix_socket=socket, user=user, passwd=password, port=port)
        else:
            if host_ip != "":
                host = host_ip
            else:
                host = parser.get(section, 'host')
            return pymysql.connect(host=host, user=user, passwd=password, port=port)
    except Exception as e:
        traceback.print_exc()


def check_read_only(cursor):
    sql = " show variables like 'read_only'"
    cursor.execute(sql)
    return cursor.fetchall()[0]["Value"]

def check_performance_schema(cursor):
    sql = " show variables like 'performance_schema'"
    cursor.execute(sql)
    return cursor.fetchall()[0]["Value"]

def check_setup_instruments_mdl(cursor):
    sql = "select * from performance_schema.setup_instruments  where NAME ='wait/lock/metadata/sql/mdl'"
    cursor.execute(sql)
    return cursor.fetchall()[0]["ENABLED"]

def check_setup_consumers_instrumentation(cursor):
    sql = "select * from performance_schema.setup_consumers  where NAME ='global_instrumentation'"
    cursor.execute(sql)
    return cursor.fetchall()[0]["ENABLED"]


def show_mdl_lock_info(rows):
    data = []
    for row in rows:
        data.append((row["ID"], row["USER"], row["HOST"], row["DB"], row["COMMAND"], row["TIME"], row["OWNER_THREAD_ID"],
                     row["OBJECT_TYPE"],row["OBJECT_SCHEMA"], row["OBJECT_NAME"],row["LOCK_TYPE"],row["LOCK_DURATION"],row["LOCK_STATUS"]))
    header = (
        "ID", "USER", "HOST", "DB", "COMMAND", "TIME",
        "OWNER_THREAD_ID", "OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME","LOCK_TYPE","LOCK_DURATION","LOCK_STATUS")
    formatted = table(data, header=header, divider=True)
    print(formatted)

# def findRootThread(cursor):
#     sql = "SELECT B.ID,B.USER,B.HOST,B.DB,b.COMMAND,b.TIME,C.OWNER_THREAD_ID,C.OBJECT_TYPE,c.OBJECT_SCHEMA,c.OBJECT_NAME," \
#           "C.LOCK_TYPE,C.LOCK_DURATION,C.LOCK_STATUS " \
#           "FROM performance_schema.threads A ,information_schema.PROCESSLIST B ," \
#           "performance_schema.metadata_locks C " \
#           "WHERE A.PROCESSLIST_ID = B.ID AND " \
#           "A.THREAD_ID = C.OWNER_THREAD_ID " \
#           "and c.LOCK_STATUS<>'PENDING' and c.OBJECT_TYPE='GLOBAL'"
#     cursor.execute(sql)
#     res = cursor.fetchall()
#
#     show_mdl_lock_info(res)

def find_waiting_root_thread(cursor):
    sql = "SELECT B.ID,B.USER,B.HOST,B.DB,b.COMMAND,b.TIME,C.OWNER_THREAD_ID,C.OBJECT_TYPE,c.OBJECT_SCHEMA," \
          "c.OBJECT_NAME,C.LOCK_TYPE,C.LOCK_DURATION,C.LOCK_STATUS " \
          "FROM performance_schema.threads A, information_schema.PROCESSLIST B," \
          "performance_schema.metadata_locks C " \
          "WHERE A.PROCESSLIST_ID = B.ID AND A.THREAD_ID = C.OWNER_THREAD_ID " \
          "and c.OWNER_THREAD_ID!=sys.ps_thread_id(connection_id())  and c.LOCK_STATUS='GRANTED'"
    print(sql)
    cursor.execute(sql)
    res = cursor.fetchall()

    show_mdl_lock_info(res)


def check_ps_mdl_lock_status(cursor):
    ps = check_performance_schema(cursor)
    mdl_enabled = check_setup_instruments_mdl(cursor)
    return ps,mdl_enabled


def show_open_tables_without_performance_schema(cursor):
    sql = "show open tables where in_use>0"
    cursor.execute(sql)
    rows = cursor.fetchall()
    res = []
    for row in rows:
        if row["Database"] != "performance_schema":
            res.append(row)
    return res


def get_bigtransactions(cursor):
    sql = "select trx_id,trx_state,trx_started,trx_requested_lock_id," \
          "trx_mysql_thread_id,trx_query,trx_tables_in_use," \
          "trx_tables_locked,trx_isolation_level,user,host,db ,info " \
          "from information_schema.innodb_trx a, information_schema.processlist b " \
          "where a.trx_mysql_thread_id=b.id " \
          "and a.trx_started < date_sub(now(), interval {0} minute)".format(BIG_TRANSACTION_TIME)
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
    except Exception as e:
        traceback.print_exc()
        cursor.close()
    sorted(rows, key=lambda keys: keys['trx_started'], reverse=True)
    return rows


def block_thread_info(cursor, id):
    check_block_sql = "SELECT r.trx_mysql_thread_id                     AS waiting_thread," \
                      "r.trx_query                                      AS waiting_query, " \
                      "r.trx_rows_modified                              AS waiting_rows_modified, " \
                      "TIMESTAMPDIFF(SECOND, r.trx_started, NOW())      AS waiting_age," \
                      "TIMESTAMPDIFF(SECOND, r.trx_wait_started, NOW()) AS waiting_wait_secs," \
                      "rp.user                                          AS waiting_user," \
                      "rp.host                                          AS waiting_host," \
                      "rp.db                                            AS waiting_db," \
                      "b.trx_mysql_thread_id                            AS blocking_thread," \
                      "b.trx_query                                      AS blocking_query," \
                      "b.trx_rows_modified                              AS blocking_rows_modified," \
                      "TIMESTAMPDIFF(SECOND, b.trx_started, NOW())      AS blocking_age," \
                      "TIMESTAMPDIFF(SECOND, b.trx_wait_started, NOW()) AS blocking_wait_secs," \
                      "bp.user                                          AS blocking_user," \
                      "bp.host                                          AS blocking_host," \
                      "bp.db                                            AS blocking_db," \
                      "CONCAT(bp.command, IF(bp.command = 'Sleep', CONCAT(' ', bp.time),   '')) AS blocking_status," \
                      "CONCAT(lock_mode, ' ', lock_type, ' ', lock_table, '(', lock_index, ')') AS lock_info " \
                      "FROM INFORMATION_SCHEMA.INNODB_LOCK_WAITS w " \
                      "JOIN INFORMATION_SCHEMA.INNODB_TRX b   ON  b.trx_id  = w.blocking_trx_id " \
                      "JOIN INFORMATION_SCHEMA.INNODB_TRX r   ON  r.trx_id  = w.requesting_trx_id " \
                      "JOIN INFORMATION_SCHEMA.INNODB_LOCKS l ON  l.lock_id = w.requested_lock_id " \
                      "LEFT JOIN INFORMATION_SCHEMA.PROCESSLIST bp ON bp.id = b.trx_mysql_thread_id " \
                      "LEFT JOIN INFORMATION_SCHEMA.PROCESSLIST rp ON rp.id = r.trx_mysql_thread_id " \
                      "where  r.trx_mysql_thread_id={0}".format(int(id))
    try:
        cursor.execute(check_block_sql)
        rows = cursor.fetchall()
    except Exception as e:
        traceback.print_exc()
        cursor.close()
    return rows


def output_sql_table_format(rows):
    data = []
    for row in rows:
        data.append((row["ID"], row["USER"], row["HOST"], row["DB"], row["COMMAND"], row["TIME"], row["STATE"],
                     row["INFO"],row["ROWS_SENT"], row["ROWS_EXAMINED"]))
    header = (
        "ID", "USER", "HOST", "DB", "COMMAND", "TIME",
        "STATE", "INFO", "ROWS_SENT", "ROWS_EXAMINED")
    formatted = table(data, header=header, divider=True)
    print(formatted)


def show_long_query(select_long_query=[], dml_long_query=[], ddl_long_query=[]):
    if len(select_long_query) > SELECT_SHOW_LIMIT:
        msg.fail("Select long query number greater than 5, so just show top 5 select sql")
    sorted(select_long_query, key=lambda keys: keys['TIME'], reverse=True)
    select_long_query = select_long_query[:5]
    if len(select_long_query) !=0:
        output_sql_table_format(select_long_query)

    if len(dml_long_query) > DML_SHOW_LIMIT:
        msg.fail("DML long query number greater than 5, so just show top 5 dml sql")
    sorted(dml_long_query, key=lambda keys: keys['TIME'], reverse=True)
    dml_long_query = dml_long_query[:5]
    if len(dml_long_query) != 0:
        output_sql_table_format(dml_long_query)

    if len(ddl_long_query) > DDL_SHOW_LIMIT:
        msg.fail("DDL long query number greater than 5, so just show top 5 ddl sql")
    sorted(ddl_long_query, key=lambda keys: keys['TIME'], reverse=True)
    ddl_long_query = ddl_long_query[:5]
    if len(ddl_long_query) !=0:
        output_sql_table_format(ddl_long_query)


def show_big_transaction(bigtrx_list):
    if len(bigtrx_list) > 5:
        msg.fail("big transaction number greater than 5, so just show top 5 dml sql")
        sorted(bigtrx_list, key=lambda keys: keys['trx_started'], reverse=True)
        bigtrx_list = bigtrx_list[:5]
    data = []
    for trx in bigtrx_list:
        data.append((trx["trx_id"], trx["trx_state"], trx["trx_started"],
                     trx["trx_requested_lock_id"], trx["trx_mysql_thread_id"], trx["trx_query"],
                     trx["trx_tables_in_use"], trx["trx_tables_locked"], trx["trx_isolation_level"],
                     trx["user"], trx["host"], trx["db"], trx["info"]))

    header = (
        "trx_id", "trx_state", "trx_started", "trx_requested_lock_id", "trx_mysql_thread_id", "trx_query",
        "trx_tables_in_use", "trx_tables_locked", "trx_isolation_level", "user", "host", "db", "info")
    formatted = table(data, header=header, divider=True)
    print(formatted)


def analyse_processlist(db, outfile, time=10):
    try:
        cursor = db.cursor(pymysql.cursors.DictCursor)
        sql = "select * from information_schema.processlist where time>={} and COMMAND <>'Sleep' ".format(time)
        cursor.execute(sql)
        results = cursor.fetchall()
    except Exception as e:
        cursor.close()
        traceback.print_exc()

    if len(results) == 0:
        print("mysql looks good")

    bigtrx = get_bigtransactions(cursor)

    dml_long_query = []
    select_long_query = []
    ddl_long_query = []
    login_list = []
    dml_count = 0
    open_table_count = 0

    for row in results:

        prefix = row["INFO"][:20].lower()
        if prefix.startswith("update") or prefix.startswith("delete") or prefix.startswith("insert") and row[
            "STATE"] in ["update", "updating", "deleting from main table", "deleting from reference tables","User sleep",
                         "executing", "updating main table", "updating reference tables","Searching rows for update"]:
            dml_long_query.append(row)
        elif prefix.startswith("select") and row["STATE"] in ["Sending data", "Copying to tmp table","User sleep",
                                                              "Copying to tmp table on disk", "Creating sort index",
                                                              "executing", "Sorting for group", "Sorting for order",
                                                              "Sorting result","removing tmp table","Sending to client"]:
            select_long_query.append(row)
        elif (prefix.startswith("alter") or prefix.startswith("drop") or prefix.startswith("truncate")) and row[
            "STATE"] in ["altering table", "copy to tmp table", "Creating index",
                         "committing alter table to storage engine", "discard_or_import_tablespace",
                         "preparing for alter table","rename"]:
            ddl_long_query.append(row)
        else:
            if row["STATE"] == "login":
                login_list.append(row)

    for row in results:
        # print >> outfile, row

        if row["STATE"] == "Rolling back":
            msg.fail("transaction is rolling back")
        if row["STATE"] == "statistics":
            msg.fail("keep statistics up to date")
        if row["STATE"] == "Creating sort index":
            msg.fail("consider optimize sql")
            msg.fail("info:", row["INFO"])
        if row["STATE"] == "Waiting for commit lock":
            msg.fail("transaction is  waiting for a commit lock.")
            ps, mdl_enabled = check_ps_mdl_lock_status(cursor)
            if ps == "ON" and mdl_enabled == "YES":
                msg.fail("try to kill thread")
                find_waiting_root_thread(cursor)

            elif mdl_enabled == "NO":
                msg.fail(
                    "wait/lock/metadata/sql/mdl does not enable, can't find which thread caused ,try to kill long sleep thread  ")

            elif ps != "ON":
                msg.fail(
                    "performance_schema does not open can't find which thread caused ,try to kill long sleep thread ")

        if row["STATE"] == "Sending data":
            msg.fail("consider enlarge buffer pool or optimize sql,maybe you query too many rows")
            msg.fail("info:",row["INFO"])
        if row["STATE"] == "Copying Waiting for global read lockto tmp table on disk":
            msg.fail("consider enlarge max_heap_table_size and tmp_table_size")
            msg.fail("info:", row["INFO"])
        if row["STATE"] == "Sending to client":
            msg.fail("Consider to enlarge  net_buffer_length or socket send buffer /proc/sys/net/core/wmem_default ,net_buffer_length  dynamically enlarged up to max_allowed_packet bytes as needed.")
        if row["STATE"] == "Waiting for global read lock":
            if row["INFO"] == "flush tables with read lock":
                msg.fail("flush tables with read lock is waiting read lock,you can kill long query and big trx")
                show_long_query(select_long_query, dml_long_query, ddl_long_query)
                show_big_transaction(bigtrx)
            else:
                open_tables_res = show_open_tables_without_performance_schema(cursor)
                if len(open_tables_res) == 0:
                    msg.fail("sql blocked by FLUSH TABLES WITH READ LOCK,try to kill thread which executed flush tables with read lock. sql is {}".format(row["INFO"]))
                    ps, mdl_enabled = check_ps_mdl_lock_status(cursor)
                    if ps == "ON" and mdl_enabled == "YES":
                        msg.fail("try to kill thred")
                        find_waiting_root_thread(cursor)

                    elif mdl_enabled == "NO":
                        msg.fail(
                            "wait/lock/metadata/sql/mdl does not enable, can't find which thread caused ,try to kill long sleep thread  ")

                    elif ps != "ON":
                        msg.fail(
                            "performance_schema does not open can't find which thread caused ,try to kill long sleep thread ")
                else:
                    msg.fail("sql blocked by other long query sql, blocked sql is :" + row["INFO"])
                    show_long_query(select_long_query, dml_long_query, ddl_long_query)
                    show_big_transaction(bigtrx)



        if row["STATE"] == "Waiting for tables":
            pass
        if row["STATE"] == "Waiting for table flush":
            msg.fail(
                "id :{}  {} is blocked by lock table statement or long queries.Waiting {} seconds ".format(row["ID"], row["INFO"],
                                                                                                row["TIME"]))
            ps,mdl_enabled = check_ps_mdl_lock_status(cursor)
            if ps == "ON" and mdl_enabled=="YES":
                msg.fail("kill it thread")
                find_waiting_root_thread(cursor)

            elif mdl_enabled!="YES":
                msg.fail(
                    "wait/lock/metadata/sql/mdl does not enable, can't find which thread caused")
            elif ps != "ON":
                msg.fail(
                    "performance_shcema does not open,so can't find which thread hold lock,try to kill long sleep thread")
            show_long_query(select_long_query,dml_long_query,ddl_long_query)
            show_big_transaction(bigtrx)

        if row["STATE"] == "Waiting for table metadata lock":
            msg.fail("id :{} {} is waiting for table metadata lock. waiting {} seconds".format(row["ID"], row["INFO"],
                                                                                               row["TIME"]))
            # show long query and check big transaction
            # query processlist according table name
            # 需要合并下，针对非常多的等待，需要找到根阻塞，可以先根据表名找慢查询，没有然后在用时间对比去找长事务
            show_long_query(select_long_query, dml_long_query, ddl_long_query)
            show_big_transaction(bigtrx)
        if row["STATE"] == "updating":
            pid = row["ID"]
            get_block_info = block_thread_info(cursor, pid)
            if len(get_block_info) > 0:
                msg.fail("DML is blocked, waiting  info is :")
                data = []
                for blockinfo in get_block_info:
                    data.append(
                        (blockinfo["waiting_thread"], blockinfo["waiting_query"], blockinfo["waiting_rows_modified"],
                         blockinfo["waiting_age"], blockinfo["waiting_wait_secs"], blockinfo["waiting_user"],
                         blockinfo["waiting_host"], blockinfo["waiting_db"], blockinfo["blocking_thread"],
                         blockinfo["blocking_query"], blockinfo["blocking_rows_modified"], blockinfo["blocking_age"],
                         blockinfo["blocking_wait_secs"], blockinfo["blocking_user"], blockinfo["blocking_host"],
                         blockinfo["blocking_db"]))

                header = (
                "waiting_thread", "waiting_query", "waiting_rows_modified", "waiting_age", "waiting_wait_secs",
                "waiting_user",
                "waiting_host", "waiting_db", "blocking_thread", "blocking_query", "blocking_rows_modified",
                "blocking_age", "blocking_wait_secs", "blocking_user", "blocking_host", "blocking_db")
                formatted = table(data, header=header, divider=True)
                print(formatted)
            else:
                dml_count = dml_count + 1
                if dml_count > 10 and len(login_list) > 5:
                    msg.fail("may be disk is full, pls check disk free space!")
                else:
                    msg.fail("long sql is executing! id: {} user: {} host: {} sql is: {}".format(row["ID"],row["USER"],row["HOST"], row["INFO"]))

        if row["STATE"] == "Opening tables":
            if len(ddl_long_query) > 0:
                msg.fail("maybe is droping or truncating big tables ,pls check big ddl ")
                show_long_query(ddl_long_query=ddl_long_query)
            open_table_count = open_table_count + 1
            if open_table_count > 10:
                msg.fail(
                    "increase table_open_cache or decrease tables in sql or decrease sql parrallel execution or disk performance is not good")




def build_option_parser():
    parser = argparse.ArgumentParser(usage='myanalyzer.py -t 5  -i 192.168.0.0.1')
    parser.add_argument(
        '-a', '--action',
        help="show or check.",
        default="check"
    )
    parser.add_argument(
        '-t', '--time',
        help="check thread which time greater than t.",
        default="10"
    )
    parser.add_argument(
        '-i', '--ip',
        help="server ip.",
        default=""
    )

    parser.add_argument(
        '-P', '--port',
        help="server port.",
        default=""
    )

    parser.add_argument(
        '-c', '--config',
        help="read MySQL configuration from. (default: '~/.my.cnf'",
        default='~/.my.cnf'
    )
    parser.add_argument(
        '-s', '--section',
        help="read MySQL configuration from this section. (default: '[client]')",
        default="client"
    )
    return parser


def main():
    parser = build_option_parser()
    opts, args = parser.parse_known_args()

    try:
        outfile = open("process.txt", "w")
    except Exception as e:
        print("openfile error:%s" % e)
        outfile.close()
    if opts.ip:
        host = opts.ip
    else:
        host = ""
    if opts.port:
        port = opts.port
    else:
        port = ""
    try:
        con = connect(opts.config, opts.section, host, port)
    except Exception as e:
        traceback.print_exc()
        con.close()



    action = opts.action
    if action == "show":
        pass
    elif action == "check":
        if opts.time:
            time = opts.time
        try:
            analyse_processlist(con, outfile, time)
        except Exception as ex:
            traceback.print_exc()
    if outfile != None:
        outfile.close()
    if con != None:
        con.close()

if __name__ == '__main__':
    main()
