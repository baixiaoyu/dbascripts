package main

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func InitMySQL(host string, port int, dbname string, username string, password string) (db *sqlx.DB, err error) {
	//dsn := "msandbox:msandbox@tcp(127.0.0.1:20996)/db1"
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, dbname)

	db, err = sqlx.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("connect server failed, err:%v\n", err)
		return
	}
	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(10)
	return db, err
}

func QueryAllTabs(db *sqlx.DB) []string {
	sql := "show tables"
	var tableNames []string
	err := db.Select(&tableNames, sql)
	if err != nil {
		fmt.Printf("query tables err:%v\n", err)

		return nil
	}

	return tableNames
}

type PkValueInfo struct {
	Min int `db:"Min"`
	Max int `db:"Max"`
}

func GetAutoIncrementPkColumn(db *sqlx.DB, db_name string, table_name string) (string, *PkValueInfo) {
	sql := fmt.Sprintf("SELECT c.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t,information_schema.TABLES AS ts, information_schema.KEY_COLUMN_USAGE AS c WHERE t.TABLE_NAME = ts.TABLE_NAME and t.table_schema=ts.table_schema and c.table_schema=t.table_schema and c.table_name=t.table_name AND ts.TABLE_NAME  = c.TABLE_NAME AND t.CONSTRAINT_TYPE = 'PRIMARY KEY' and ts.AUTO_INCREMENT is not null and t.TABLE_NAME='%s' and t.table_schema='%s' ", table_name, db_name)
	var pkNames []string
	err := db.Select(&pkNames, sql)
	if err != nil {
		fmt.Printf("query tables err:%v\n", err)

		return "", nil
	}
	//fmt.Printf("pkNmaes:%+v", pkNames)
	var pkinfo []PkValueInfo
	sql = fmt.Sprintf("select min(%s) as Min, max(%s) as Max from %s.%s", pkNames[0], pkNames[0], db_name, table_name)

	err = db.Select(&pkinfo, sql)
	if err != nil {
		fmt.Printf("query tables err:%v\n", err)

		return "", nil
	}
	return pkNames[0], &(pkinfo[0])

}

func FindDiffrentRows(crcdb *sqlx.DB, sourcedb *sqlx.DB, targetdb *sqlx.DB) {
	sql := "select a.dbname,a.table_name,a.chunk_num ,a.start_pk as startpk ,a.end_pk as endpk from (select * from crc_result where which='source' )a, (select * from crc_result where which='target') b where    a.table_name=b.table_name and a.chunk_num=b.chunk_num and (a.crc <> b.crc or  a.cnt<> b.cnt)"
	var diffResult []CrcResult
	err := crcdb.Select(&diffResult, sql)
	if err != nil {
		fmt.Printf("find diff rows err:%v\n", err)
	}
	for _, row := range diffResult {

		Target_column_string = ""
		Source_column_string = ""

		source_all_column := GetCols(sourcedb, Config.SourceMySQLDB, row.Tab_name)
		target_all_column := GetCols(targetdb, Config.TargetMySQLDB, row.Tab_name)

		for _, value := range source_all_column {
			Source_column_string = fmt.Sprintf("%s%s,", Source_column_string, value)

		}
		Source_column_string = Source_column_string[:len(Source_column_string)-1]

		for _, value := range target_all_column {
			//Target_column_string = Target_column_string + value + ","
			Target_column_string = fmt.Sprintf("%s%s,", Target_column_string, value)

		}
		Target_column_string = Target_column_string[:len(Target_column_string)-1]

		sourcePkColumn, _ := GetAutoIncrementPkColumn(sourcedb, Config.SourceMySQLDB, row.Tab_name)
		targetPkColumn, _ := GetAutoIncrementPkColumn(targetdb, Config.TargetMySQLDB, row.Tab_name)

		chunk_number := row.Chunk_num
		start_pk := row.Start_pk
		end_pk := row.End_pk
		table_name := row.Tab_name
		for i := start_pk; i <= end_pk; i++ {

			source_condition := fmt.Sprintf("where %s >=%d and %s < %d", sourcePkColumn, i, sourcePkColumn, i+1)
			source_sql := fmt.Sprintf("select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk ,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", Config.SourceMySQLDB, table_name, chunk_number, start_pk, end_pk, Source_column_string, Config.SourceMySQLDB, table_name, source_condition)
			sourceCrcResult := QueryChecksum(source_db, source_sql)

			target_condition := fmt.Sprintf("where %s >=%d and %s < %d", targetPkColumn, i, targetPkColumn, i+1)
			target_sql := fmt.Sprintf("select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", Config.SourceMySQLDB, table_name, chunk_number, start_pk, end_pk, Target_column_string, Config.TargetMySQLDB, table_name, target_condition)
			targetCrcResult := QueryChecksum(target_db, target_sql)

			if sourceCrcResult.Crc != targetCrcResult.Crc {
				fmt.Printf("find diffrent rows for table:%s in chunk_num:%d\n", row.Tab_name, row.Chunk_num)
				println("find diffrent rows id:", i)
			}

		}
	}

}

func RecordCrcResult(db *sqlx.DB, crcresult *CrcResult, which string) {
	insertsql := `INSERT INTO crc_result (dbname, table_name,chunk_num,start_pk,end_pk,crc,cnt,ctime,which) VALUES (?, ?, ?, ?, ?, ?, ?, now(),?)`
	db.MustExec(insertsql, crcresult.Db_name, crcresult.Tab_name, crcresult.Chunk_num, crcresult.Start_pk, crcresult.End_pk, crcresult.Crc, crcresult.Cnt, which)

}
func RecreateCrcTable(db *sqlx.DB) {
	sql := "create table if not exists  `crc_result` (`id` int(11) NOT NULL AUTO_INCREMENT,`dbname` varchar(100) NOT NULL DEFAULT '',`table_name` varchar(50) NOT NULL DEFAULT '',`chunk_num` int(11) NOT NULL,`start_pk` int(11) NOT NULL,`end_pk` int(11) NOT NULL,`crc` varchar(100) DEFAULT NULL,`cnt` int(11) DEFAULT NULL,`ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,`which` varchar(10) DEFAULT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 "

	err := db.MustExec(sql)
	if err != nil {
		fmt.Printf("create crc tables err:%v\n", err)
	}
	sql = "delete from crc_result"
	err = db.MustExec(sql)
	if err != nil {
		fmt.Printf("delete crc tables err:%v\n", err)
	}

}

func GetCols(db *sqlx.DB, db_name string, table_name string) []string {
	sql := fmt.Sprintf("select COLUMN_NAME from information_schema.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'", db_name, table_name)
	var columnNames []string
	err := db.Select(&columnNames, sql)
	if err != nil {
		fmt.Printf("query tables err:%v\n", err)
		return nil
	}
	return columnNames
}

type CrcResult struct {
	Db_name   string `db:"dbname"`
	Tab_name  string `db:"table_name"`
	Chunk_num int    `db:"chunk_num"`
	Start_pk  int    `db:"startpk"`
	End_pk    int    `db:"endpk"`
	Cnt       int    `db:"cnt"`
	Crc       string `db:"crc"`
}

func QueryChecksum(db *sqlx.DB, sql string) *CrcResult {

	var crcResult []CrcResult
	err := db.Select(&crcResult, sql)
	if err != nil {
		fmt.Printf("query tables err:%v\n", err)

		return nil
	}

	return &(crcResult[0])
}

func prepareQueryDemo(db *sqlx.DB, starttime string, endtime string) {
	sqlStr := "select * from sbtest1 where utime> ? and utime < ?"
	stmt, err := db.Prepare(sqlStr)
	if err != nil {
		fmt.Printf("prepare failed, err:%v\n", err)
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query(starttime, endtime)
	if err != nil {
		fmt.Printf("query failed, err:%v\n", err)
		return
	}
	defer rows.Close()
	// // 循环读取结果集中的数据
	// for rows.Next() {
	// 	var u user
	// 	err := rows.Scan(&u.id, &u.name, &u.age)
	// 	if err != nil {
	// 		fmt.Printf("scan failed, err:%v\n", err)
	// 		return
	// 	}
	// 	fmt.Printf("id:%d name:%s age:%d\n", u.id, u.name, u.age)
	// }
}
