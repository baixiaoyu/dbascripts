package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

var source_db *sqlx.DB
var target_db *sqlx.DB
var crc_db *sqlx.DB
var Source_column_string string
var Target_column_string string
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
	//对比自增主键是否一致
	println("begin compare")

	defer wg.Done()

	sourcePkColumn, sourcePkInfo := GetAutoIncrementPkColumn(source_db, Config.SourceMySQLDB, table_name)
	targetPkColumn, targetPkInfo := GetAutoIncrementPkColumn(target_db, Config.TargetMySQLDB, table_name)

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
	source_all_column := GetCols(source_db, Config.SourceMySQLDB, table_name)
	target_all_column := GetCols(target_db, Config.TargetMySQLDB, table_name)

	matex.Lock()
	Target_column_string = ""
	Source_column_string = ""
	for _, value := range source_all_column {
		Source_column_string = fmt.Sprintf("%s%s,", Source_column_string, value)

	}
	Source_column_string = Source_column_string[:len(Source_column_string)-1]

	for _, value := range target_all_column {
		//Target_column_string = Target_column_string + value + ","
		Target_column_string = fmt.Sprintf("%s%s,", Target_column_string, value)

	}
	Target_column_string = Target_column_string[:len(Target_column_string)-1]

	row_count := sourcePkInfo.Max - sourcePkInfo.Min + 1
	round := row_count / chunk_size

	println("table name :", table_name)
	var chunk_number int
	var start_pk int
	var end_pk int
	for i := 1; i <= round; i++ {

		chunk_number = i
		start_pk = (i - 1) * chunk_size
		end_pk = start_pk + chunk_size
		source_condition := fmt.Sprintf("where %s >=%d and %s <= %d", sourcePkColumn, start_pk, sourcePkColumn, end_pk)
		source_sql := fmt.Sprintf("select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk ,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", Config.SourceMySQLDB, table_name, chunk_number, start_pk, end_pk, Source_column_string, Config.SourceMySQLDB, table_name, source_condition)

		sourceCrcResult := QueryChecksum(source_db, source_sql)
		RecordCrcResult(crc_db, sourceCrcResult, "source")
		target_condition := fmt.Sprintf("where %s >=%d and %s <= %d", targetPkColumn, start_pk, targetPkColumn, end_pk)
		target_sql := fmt.Sprintf("select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", Config.SourceMySQLDB, table_name, chunk_number, start_pk, end_pk, Target_column_string, Config.TargetMySQLDB, table_name, target_condition)

		targetCrcResult := QueryChecksum(target_db, target_sql)
		RecordCrcResult(crc_db, targetCrcResult, "target")

	}
	if end_pk < sourcePkInfo.Max {

		if chunk_number == 0 {
			start_pk = 1
		} else {
			start_pk = chunk_number * chunk_size
		}

		end_pk = sourcePkInfo.Max

		source_condition := fmt.Sprintf("where %s >%d ", sourcePkColumn, start_pk)
		source_sql := fmt.Sprintf("select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", Config.SourceMySQLDB, table_name, chunk_number, start_pk, end_pk, Source_column_string, Config.SourceMySQLDB, table_name, source_condition)

		sourceCrcResult := QueryChecksum(source_db, source_sql)

		RecordCrcResult(crc_db, sourceCrcResult, "source")

		target_condition := fmt.Sprintf("where %s >%d ", targetPkColumn, start_pk)
		target_sql := fmt.Sprintf("select '%s' as dbname ,'%s' as table_name,'%d' as chunk_num,'%d' as startpk,'%d' as endpk,count(*) as cnt,COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc from %s.%s force index(`PRIMARY`) %s", Config.TargetMySQLDB, table_name, chunk_number, start_pk, end_pk, Target_column_string, Config.TargetMySQLDB, table_name, target_condition)

		targetCrcResult := QueryChecksum(target_db, target_sql)

		RecordCrcResult(crc_db, targetCrcResult, "target")
	}
	matex.Unlock()
}

func main() {
	ForceRead("db.conf.json")

	source_db, _ = InitMySQL(Config.SourceMySQLHost, Config.SourceMySQLPort, Config.SourceMySQLDB, Config.SourceMySQLUser, Config.SourceMySQLUserPassword)
	target_db, _ = InitMySQL(Config.TargetMySQLHost, Config.TargetMySQLPort, Config.TargetMySQLDB, Config.TargetMySQLUser, Config.TargetMySQLUserPassword)

	crc_db, _ = InitMySQL(Config.ChecksumHost, Config.ChecksumPort, Config.ChecksumDB, Config.ChecksumUser, Config.ChecksumPassword)

	RecreateCrcTable(crc_db)

	source_tables := QueryAllTabs(source_db)
	//fmt.Printf("source tables:%#v\n", source_tables)

	target_tables := QueryAllTabs(target_db)
	//fmt.Printf("target tables:%#v\n", target_tables)

	not_in_target := SubtrDemo(source_tables, target_tables)
	not_in_source := SubtrDemo(target_tables, source_tables)

	fmt.Printf("table not in target:%+v\n", not_in_target)
	fmt.Printf("table not in source:%+v\n", not_in_source)

	common_tables := Intersection(source_tables, target_tables)
	fmt.Printf("table  in common:%+v\n", common_tables)

	t := time.Now()
	for _, tab_name := range common_tables {
		wg.Add(1)
		go CompareTable(tab_name)
	}
	wg.Wait()
	elapsed := time.Since(t)
	fmt.Println("table compare elapsed:", elapsed)

	FindDiffrentRows(crc_db, source_db, target_db)

}
