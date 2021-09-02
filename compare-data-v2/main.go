package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

var Source_db *sqlx.DB
var Target_db *sqlx.DB
var Crc_db *sqlx.DB
var Source_column_string string
var Target_column_string string
var Target_nullable_column_string string
var Source_nullable_column_string string

var wg sync.WaitGroup
var SourcePkColumn string
var TargetPkColumn string
var matex sync.Mutex

const (
	chunk_size = 1000
)

func init() {

}

func CompareTable(table_name string) {
	println("begin compare")

	defer wg.Done()

	sourcePkColumn, sourcePkInfo := GetAutoIncrementPkColumn(Source_db, Config.SourceMySQLDB, table_name)
	targetPkColumn, targetPkInfo := GetAutoIncrementPkColumn(Target_db, Config.TargetMySQLDB, table_name)

	SourcePkColumn = sourcePkColumn
	TargetPkColumn = targetPkColumn

	if sourcePkColumn != targetPkColumn {
		println("primary key name not same")
		return
	}

	if sourcePkInfo.Min != targetPkInfo.Min || sourcePkInfo.Max != targetPkInfo.Max {
		println("primary key min or max not same")
		return
	}

	// 根据主键批量计算checksum,记录checksum到文件
	source_all_column := GetCols(Source_db, Config.SourceMySQLDB, table_name)
	target_all_column := GetCols(Target_db, Config.TargetMySQLDB, table_name)

	source_isnullable_column := GetIsNullableCols(Source_db, Config.SourceMySQLDB, table_name)
	target_isnullable_column := GetIsNullableCols(Target_db, Config.TargetMySQLDB, table_name)

	matex.Lock()
	Target_column_string = ""
	Source_column_string = ""

	Target_nullable_column_string = ""
	Source_nullable_column_string = ""

	for _, value := range source_all_column {
		Source_column_string = fmt.Sprintf("%s%s,", Source_column_string, value)
	}
	Source_column_string = Source_column_string[:len(Source_column_string)-1]

	for _, value := range target_all_column {
		Target_column_string = fmt.Sprintf("%s%s,", Target_column_string, value)
	}
	Target_column_string = Target_column_string[:len(Target_column_string)-1]

	for _, value := range source_isnullable_column {
		Source_nullable_column_string = fmt.Sprintf("%s%s,", Source_nullable_column_string, value)
	}
	if len(source_isnullable_column) > 0 {
		Source_nullable_column_string = Source_nullable_column_string[:len(Source_nullable_column_string)-1]
	}

	for _, value := range target_isnullable_column {
		Target_nullable_column_string = fmt.Sprintf("%s%s,", Target_nullable_column_string, value)
	}
	if len(target_isnullable_column) > 0 {
		Target_nullable_column_string = Target_nullable_column_string[:len(Target_nullable_column_string)-1]

	}

	row_count := sourcePkInfo.Max - sourcePkInfo.Min + 1
	round := row_count / chunk_size

	println("table name :", table_name)
	var chunk_number int
	var start_pk int
	var end_pk int
	var source_sql string
	var target_sql string

	for i := 1; i <= round; i++ {

		chunk_number = i
		start_pk = (i - 1) * chunk_size
		end_pk = start_pk + chunk_size
		source_condition := fmt.Sprintf("where %s >=%d and %s <= %d", sourcePkColumn, start_pk, sourcePkColumn, end_pk)
		if len(source_isnullable_column) > 0 {
			source_sql = fmt.Sprintf("%s:select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk ,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s,CONCAT(%s))) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", "source", Config.SourceMySQLDB, table_name, chunk_number, start_pk, end_pk, Source_column_string, Source_nullable_column_string, Config.SourceMySQLDB, table_name, source_condition)

		} else {
			source_sql = fmt.Sprintf("%s:select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk ,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", "source", Config.SourceMySQLDB, table_name, chunk_number, start_pk, end_pk, Source_column_string, Config.SourceMySQLDB, table_name, source_condition)
		}
		DiscoveryQueue.Push(source_sql)

		target_condition := fmt.Sprintf("where %s >=%d and %s <= %d", targetPkColumn, start_pk, targetPkColumn, end_pk)
		if len(target_isnullable_column) > 0 {
			target_sql = fmt.Sprintf("%s:select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s,CONCAT(%s))) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", "target", Config.SourceMySQLDB, table_name, chunk_number, start_pk, end_pk, Target_column_string, Target_nullable_column_string, Config.TargetMySQLDB, table_name, target_condition)

		} else {
			target_sql = fmt.Sprintf("%s:select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", "target", Config.SourceMySQLDB, table_name, chunk_number, start_pk, end_pk, Target_column_string, Config.TargetMySQLDB, table_name, target_condition)

		}
		DiscoveryQueue.Push(target_sql)

	}
	if end_pk < sourcePkInfo.Max {

		if chunk_number == 0 {
			start_pk = 1
		} else {
			start_pk = chunk_number * chunk_size
		}

		end_pk = sourcePkInfo.Max

		source_condition := fmt.Sprintf("where %s >%d ", sourcePkColumn, start_pk)
		if len(source_isnullable_column) > 0 {
			source_sql = fmt.Sprintf("%s:select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s,CONCAT(%s))) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", "source", Config.SourceMySQLDB, table_name, chunk_number, start_pk, end_pk, Source_column_string, Source_nullable_column_string, Config.SourceMySQLDB, table_name, source_condition)
		} else {
			source_sql = fmt.Sprintf("%s:select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", "source", Config.SourceMySQLDB, table_name, chunk_number, start_pk, end_pk, Source_column_string, Config.SourceMySQLDB, table_name, source_condition)
		}
		DiscoveryQueue.Push(source_sql)

		target_condition := fmt.Sprintf("where %s >%d ", targetPkColumn, start_pk)
		if len(target_isnullable_column) > 0 {
			target_sql = fmt.Sprintf("%s:select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s,CONCAT(%s))) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", "target", Config.TargetMySQLDB, table_name, chunk_number, start_pk, end_pk, Target_column_string, Target_nullable_column_string, Config.TargetMySQLDB, table_name, target_condition)

		} else {
			target_sql = fmt.Sprintf("%s:select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", "target", Config.TargetMySQLDB, table_name, chunk_number, start_pk, end_pk, Target_column_string, Config.TargetMySQLDB, table_name, target_condition)
		}
		DiscoveryQueue.Push(target_sql)
	}
	matex.Unlock()
}

func main() {
	ForceRead("db.conf.json")

	Source_db, _ = InitMySQL(Config.SourceMySQLHost, Config.SourceMySQLPort, Config.SourceMySQLDB, Config.SourceMySQLUser, Config.SourceMySQLUserPassword)
	Target_db, _ = InitMySQL(Config.TargetMySQLHost, Config.TargetMySQLPort, Config.TargetMySQLDB, Config.TargetMySQLUser, Config.TargetMySQLUserPassword)

	SetISOLATION(Source_db)
	SetISOLATION(Target_db)

	stx, err := Source_db.Beginx() // 开启事务
	if err != nil {
		fmt.Printf("source dbbegin trans failed, err:%v\n", err)
	}

	ttx, err := Target_db.Beginx() // 开启事务
	if err != nil {
		fmt.Printf("source dbbegin trans failed, err:%v\n", err)
	}
	defer func() {
		println("done")
		stx.Commit()
		ttx.Commit()
		Source_db.Close()
		Target_db.Close()
		Crc_db.Close()

	}()

	Crc_db, _ = InitMySQL(Config.ChecksumHost, Config.ChecksumPort, Config.ChecksumDB, Config.ChecksumUser, Config.ChecksumPassword)

	RecreateCrcTable(Crc_db)

	source_tables := QueryAllTabs(Source_db)
	//fmt.Printf("source tables:%#v\n", source_tables)

	target_tables := QueryAllTabs(Target_db)
	//fmt.Printf("target tables:%#v\n", target_tables)

	not_in_target := SubtrDemo(source_tables, target_tables)
	not_in_source := SubtrDemo(target_tables, source_tables)

	fmt.Printf("table not in target:%+v\n", not_in_target)
	fmt.Printf("table not in source:%+v\n", not_in_source)

	var common_tables []string
	if Config.CheckTables == "*" {
		common_tables = Intersection(source_tables, target_tables)
	} else {
		common_tables = strings.Split(Config.CheckTables, ",")
	}

	fmt.Printf("table  in common:%+v\n", common_tables)
	go HandleDiscoveryRequests()
	t := time.Now()
	for _, tab_name := range common_tables {
		wg.Add(1)
		go CompareTable(tab_name)
	}

	wg.Wait()
	elapsed := time.Since(t)
	time.Sleep(time.Duration(2) * time.Second)
	fmt.Println("table compare elapsed:", elapsed)

	//time.Sleep(time.Duration(100) * time.Second)
	for {
		if Counter == 0 {
			StopMonitoring()
			FindDiffrentRows(Crc_db, Source_db, Target_db)
			return
		}
	}
}
