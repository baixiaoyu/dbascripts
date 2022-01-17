# -*- coding: utf-8 -*-
#!/usr/bin/python
# Copyright (2014) Thomas Huang

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import pymysql
import pprint
import re

logging.basicConfig(
    level = logging.INFO,
    format = '%(message)s',
    datefmt = '%a, %Y-%m-%d %H:%M:%S',
    filename = 'table_diff.log',
    filemode = 'w'
)


def get_con(host, db,port, user, passwd):
    con = None
    try:
        con = pymysql.connect(host= host,db=db,port=port, user= user,passwd= passwd,charset="utf8mb4")

    except pymysql.Error, e:
        print "MySQL Connect Error: %s" % (e.args[1])
    return con


def get_tables(con):
    cur = con.cursor()
    cur.execute("SHOW TABLES")
    tables = [t[0] for t in cur.fetchall()]
    cur.close()
    return tables


def get_table_desc(con, table_name):

    cur = con.cursor(pymysql.cursors.DictCursor)
    cur.execute("desc %s" % (table_name))
    desc = cur.fetchall()
    for d in desc:
        pprint.pprint(Column(d))
    cur.close()
    print desc


class Database:

    def __init__(self, host, db, port,user, passwd):
        self.db = db
        self.host = host
        self.con = get_con(host, db, port,user, passwd)
        self.init_tables()

    def get_tables(self):
        tables = set()
        for t in self.tables:
            tables.add(t)
        return tables

    def printf(self):
        print "Database : %s\n" % (self.db)
        for t in self.tables:
            print t

    def __repr__(self):
        db_desc = "Database : %s\n" % (self.db)
        tables_desc = "\n".join([str(t) for t in self.tables])

        return db_desc + tables_desc

    def init_tables(self):
        self.tables = {}
        cur = self.con.cursor()
        try:
            cur.execute("SHOW TABLES")
            tables = [t[0] for t in cur.fetchall()]
            # pp = pprint.PrettyPrinter(indent=4)
            # pp.pprint(tables)
            print "Loading tables on %s/%s..." % (self.host, self.db)
            for t in tables:
                cols = self.fetch_table_columns(t)
                indexes = self.fetch_table_indexes(t)
                sql = self.fetch_sql(t)
                self.tables[t] = Table(t, cols, indexes, sql)
        finally:
            cur.close()

    def fetch_table_columns(self, table_name):
        columns = {}
        cur = self.con.cursor(pymysql.cursors.DictCursor)
        try:
            cur.execute("desc %s" % (table_name))
            for c in cur.fetchall():
                columns[c['Field']] = c
        finally:
            cur.close()
        return columns

    def fetch_table_indexes(self, table_name):
        indexes = {}
        cur = self.con.cursor(pymysql.cursors.DictCursor)
        try:
            cur.execute("show index in %s" % (table_name))
            for index in cur.fetchall():
                i = indexes.get(index['Key_name'], None)
                if i:
                    i['Columns'] += ", " + index['Column_name']
                    i['Null'] += ", " + index['Null']
                else:
                    col_name = index['Column_name']
                    del index['Column_name']
                    index['Columns'] = col_name
                    indexes[index['Key_name']] = Index(index)
        finally:
            cur.close()
                # pprint.pprint(indexes)
        return indexes

    def fetch_sql(self, table):
        cur = self.con.cursor()
        cur.execute("SHOW CREATE TABLE %s" % (table))
        create_sql = cur.fetchone()[1]
        cur.close()
        return create_sql

    def get_all(self,sql):
        cur = self.con.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        cur.close()
        return res

    def __del__(self):
        self.con.close()


class Table:

    def __init__(self, table_name, cols, indexes, sql):
        self.table_name = table_name
        self.columns = cols
        self.indexes = indexes
        self.sql = sql

        self.charset = ""
        self.collate = ""
        self.auto_increment = ""
        self.engine = ""
        self.init_table(sql)
#     def __eq__(self, other):
#         return self.table_name == other.table_name

    def init_table(self,sql):
        res = sql.split("\n")[-1].split(" ")
        print(res)
        for i in res:
            i = i.encode("utf-8")
            if i.find("ENGINE")>=0:
                self.engine = i.split("=")[-1]
            elif i.find("AUTO_INCREMENT")>=0:
                self.auto_increment = i.split("=")[-1]
                print("self.auto_increment",self.auto_increment)

            elif i.find("CHARSET")>=0:
                self.charset = i.split("=")[-1]

            elif i.find("COLLATE")>=0:
                self.collate = i.split("=")[-1]





    @property
    def columns_set(self):
        cols = set()
        for c in self.columns:
            cols.add(c)
        return cols

    @property
    def indexes_set(self):
        indexes = set()
        for c in self.indexes:
            indexes.add(c)
        return indexes

    def _get_index_lines(self):
        index_lines = []
        lines = self.sql.split("\n")

        for line in lines:
            if "KEY" in line:
                index_lines.append(line.strip(", "))
        return index_lines

    index_lines = property(_get_index_lines)

    def col_desc(self, column):

        column_regex = re.compile(r'\s+(`*%s`*[^,\n\r]*)' % (column))
        colunm_lines = column_regex.findall(self.sql)
        return colunm_lines[0]

    def __str__(self):
        return "<Tables : %s>\n%s\n" % (self.table_name,
                                        "\n".join([str(c) for c in self.columns]))

    def __repr__(self):
        return "<Tables : %s>\n%s\n" % (self.table_name,
                                        "\n".join([str(c) for c in self.columns]))


class Column:

    def __init__(self, desc):
        self.desc = desc

    def __getitem__(self, name):
        return self.desc[name]

    def __str__(self):
        return "\t%s %s" % (self.desc['Field'], self.desc['Type'])

    def __repr__(self):
        return "\t%s %s" % (self.desc['Field'], self.desc['Type'])

    def __eq__(self, other):
        for d in self.desc:
            if self.desc[d] != other.desc[d]:
                return False
        return True


class Index:

    def __init__(self, desc):
        self.desc = desc

    def __getitem__(self, name):
        return self.desc[name]

    def __setitem__(self, key, name):
        self.desc[key] = name

    def __str__(self):
        return str(self.desc)

    def __repr__(self):
        return str(self.desc)

    def __eq__(self, other):
        for d in self.desc:
            if self.desc[d] != other.desc[d]:
                return False
        return True


class MySQLDiff:
    res = ''

    def __init__(self, db1, db2):
        self.db1 = db1
        self.db2 = db2
        self.db1_tables = self.db1.get_tables()
        self.db2_tables = self.db2.get_tables()

    def diff(self):
        print "Diff between %s:%s/%s and %s:%s/%s\n" % (
            self.db1.host, self.db1.con.port,self.db1.db, self.db2.host, self.db2.con.port,self.db2.db)
        self.compare_tables(self.db1, self.db2)
        self.compare_tables(self.db2, self.db1)
        self.compare_tables_colunms()
        return self.res

    def compare_tables(self, db1, db2):
        miss_tables = db1.get_tables() - db2.get_tables()
        if len(miss_tables):
            self.res = ''.join(self.res) + "In %s:<%s> doesn't exist table(s):\n\t" % (db2.host, db2.db)
            self.res = ''.join(self.res) + "\t", "\n\t\t".join(miss_tables)+'\n'

    def compare_tables_colunms(self):
        inter_tables = self.db1_tables & self.db2_tables
        for t in inter_tables:
            self.res = ''.join(self.res) + "\n\n --------------------Begin to compare table structure In table: %s -----------------------\n" % (t)
            t1 = self.db1.tables[t]
            t2 = self.db2.tables[t]
            self.compare_columns(self.db2, t1, t2, t)
            self.compare_columns(self.db1, t2, t1, t)
            self.diff_columns(t1, t2)

            self.compare_indexes(self.db2, t1, t2, t)

            self.compare_indexes(self.db1, t2, t1, t)
            self.diff_indexes(t1, t2)

            self.diff_engine_auto_increment_charset(t1,t2,t)
            self.res = ''.join(self.res) + ("\n")

    def diff_engine_auto_increment_charset(self, t1,t2,t):

        if t1.engine != t2.engine:
            self.res = self.res + "\t\n On table:%s engine is not same:\t %s:%s/%s is %s\t  %s:%s/%s is %s\n" % (t,self.db1.host,self.db1.con.port,self.db1.db,t1.engine,self.db2.host,self.db2.con.port,self.db2.db,t2.engine)
        if t1.auto_increment != t2.auto_increment:
            self.res = self.res + "\t\n auto increment is not same:\t %s:%s/%s is %s \t %s:%s/%s is %s \n" % (self.db1.host,self.db1.con.port,self.db1.db,t1.auto_increment,self.db2.host,self.db2.con.port,self.db2.db,t2.auto_increment)
        if t1.charset != t2.charset:
            self.res = self.res + "\t\n charset is not same:\t %s:%s/%s is %s \t %s:%s/%s is %s \n" % (self.db1.host,self.db1.con.port,self.db1.db,t1.charset,self.db2.host,self.db2.con.port,self.db2.db,t2.charset)
        if t1.collate != t2.collate:
            self.res = self.res + "\t\n COLLATE is not same:\t %s:%s/%s is %s \t %s:%s/%s is %s\n" % (self.db1.host,self.db1.con.port,self.db1.db,t1.collate,self.db2.host,self.db2.con.port,self.db2.db,t2.collate)


    def compare_columns(self, db, t1, t2,t):
        miss_cols = t1.columns_set - t2.columns_set
        if miss_cols:

            self.res = self.res + "\t\n On %s:%s/%s table:%s doesn't exist colunm(s):\t\n" % (db.host, db.con.port,db.db,t )
            for col in miss_cols:
                self.res = self.res + "\t", col.rjust(20), ": ", t1.col_desc(col)
                self.res = ''.join(self.res) + "\n"

    def diff_columns(self, t1, t2):
        inter_cols = t1.columns_set & t2.columns_set
        for col in inter_cols:
            t1_col = t1.columns[col]
            t2_col = t2.columns[col]

            diff_keys = self.col_diff_keys(t1_col, t2_col)
            if len(diff_keys):
                self.res = ''.join(self.res) + "\t\nColunm *%s* is different:\n" % (t1_col['Field'])
                self.res = ''.join(self.res) + "\t\n", self.db1.host.rjust(20),":", str(self.db1.con.port), "/",self.db1.db, ": ", t1.col_desc(col)
                self.res = ''.join(self.res) + "\t\n", self.db2.host.rjust(20),":", str(self.db2.con.port), "/", self.db2.db, ": ", t2.col_desc(col)
                for key in diff_keys:
                    self.res = ''.join(self.res) + "\n", ("different %s" % (key)).rjust(20), ": *%s* *%s*" % \
                        (t1_col[key], t2_col[key])
                self.res = ''.join(self.res) +("\n")

#             for key in ['Type', 'Null', 'Key', 'Default', 'Extra']:
#                 self.cmp_col_key(t1_col, t2_col, key)

    def col_diff_keys(self, col1, col2):
        diff_keys = []
        for key in ['Type', 'Null', 'Key', 'Default', 'Extra']:
            if col1[key] != col2[key]:
                diff_keys.append(key)
        return diff_keys

#     def cmp_col_key(self, col1, col2, key):
#         if col1[key] != col2[key]:
#             print "\tColumn *%s* is different %s: *%s* *%s*" % \
#                 (col1['Field'], key, col1[key], col2[key])
#             return False
# else:
# print "%s : %s" % (col, t1_col['Type'])
#             return True

    def compare_indexes(self, db, t1, t2,t):
        miss_indexes = t1.indexes_set - t2.indexes_set
        if miss_indexes:
            # self.res = self.res + " \tIn table %s\n-----------------------\n" % (t)
            self.res = self.res + "\t\nOn %s:%s/%s doesn't exist index(es):" % (db.host, db.con.port,db.db)
            for index in miss_indexes:
                line = self.find_line(t1.index_lines, index)
                self.res = ''.join(self.res) + "\t\n", index.rjust(20), ": ", line
            self.res = ''.join(self.res) + "\n"

    def diff_indexes(self, t1, t2):
        inter_indexes = t1.indexes_set & t2.indexes_set

        index_names = []
        for i in inter_indexes:
            t1_index = t1.indexes[i]
            t2_index = t2.indexes[i]

            if len(self.cmp_index_key(t1_index, t2_index)):
                index_names.append(t1_index['Key_name'])

        if len(index_names):
            lines1 = t1.index_lines
            lines2 = t2.index_lines
            for name in index_names:
                line = self.find_line(lines1, name)
                if line:
                    self.res = ''.join(self.res) + "\t", self.db1.host.rjust(20), ": ", line
                line = self.find_line(lines2, name)
                if line:
                    self.res = ''.join(self.res) + "\t", self.db2.host.rjust(20), ": ", line

    def find_line(self, lines, name):
        for line in lines:
            if name in line:
                return line
        return None

    def cmp_index_key(self, i1, i2):
        key_names = []
        for key in ['Columns', 'Null']:
            if i1[key] != i2[key]:
                self.res = self.res + "\tIndex *%s* is different %s: *%s* *%s*" % \
                    (i1['Key_name'], key, i1[key], i2[key])
                key_names.append(key)
        return key_names


def get_commandline_options():
    from argparse import ArgumentParser
    parser = ArgumentParser(usage="usage: PROG [options]  mysql_urls...]")
    parser.add_argument(
        "mysql_urls", default=[], nargs='*', help="the mysqls to diff")
    return parser.parse_args()


def parse_mysql_url(mysql_url):
    user_info, url = mysql_url.split("@")
    host_port, db = url.split("/")
    host,port = host_port.split(":")
    port = int(port)
    user_info = user_info.split(":", 1)

    if len(user_info) != 2:
        import getpass
        print "Input password for %s@%s" % (user_info[0], host)
        user_info.append(getpass.getpass())
    user, passwd = user_info
    return host, db, port,user, passwd


if __name__ == '__main__':

    print("\nStarting diff ........\n")
    src_url = "msandbox:msandbox@127.0.0.1:20996/u1"
    dest_url = "msandbox:msandbox@127.0.0.1:20996/u2"

    src = Database(*parse_mysql_url(src_url))
    dest = Database(*parse_mysql_url(dest_url))

    db_diff = MySQLDiff(src, dest)
    # str = "## Diff between %s/%s/%s and %s/%s/%s\n" % (src.host, src.con.port, src.db, dest.host,dest.con.port,dest.db)

    res = db_diff.diff()
    logging.info(res)
    logging.info("\n")



