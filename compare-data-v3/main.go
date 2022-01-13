package main

import (
	gosql "database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

var Source_db *gosql.DB
var Target_db *gosql.DB
var Crc_db *gosql.DB
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

func main() {
	ForceRead("db.conf.json")

	sourceInstance := InstanceKey{Hostname: Config.SourceMySQLHost, Port: Config.SourceMySQLPort}
	targetInstance := InstanceKey{Hostname: Config.TargetMySQLHost, Port: Config.TargetMySQLPort}
	crcInstance := InstanceKey{Hostname: Config.ChecksumHost, Port: Config.ChecksumPort}

	sourceConfig := NewConnectionConfig()
	sourceConfig.Key = sourceInstance
	sourceConfig.User = Config.SourceMySQLUser
	sourceConfig.Password = Config.SourceMySQLUserPassword

	targetConfig := NewConnectionConfig()
	targetConfig.Key = targetInstance
	targetConfig.User = Config.TargetMySQLUser
	targetConfig.Password = Config.TargetMySQLUserPassword

	crcConfig := NewConnectionConfig()
	crcConfig.Key = crcInstance
	crcConfig.User = Config.ChecksumUser
	crcConfig.Password = Config.ChecksumPassword

	Source_db, _ = GetDB(sourceConfig, Config.SourceMySQLDB)
	Target_db, _ = GetDB(targetConfig, Config.TargetMySQLDB)
	Crc_db, _ = GetDB(crcConfig, Config.ChecksumDB)

	SetISOLATION(Source_db)
	SetISOLATION(Target_db)

	stx, err := Source_db.Begin() // 开启事务
	if err != nil {
		fmt.Printf("source dbbegin trans failed, err:%v\n", err)
	}

	ttx, err := Target_db.Begin() // 开启事务
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

	source_tables, _ := GetTables(Source_db, Config.SourceMySQLDB)
	target_tables, _ := GetTables(Target_db, Config.TargetMySQLDB)

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
	t := time.Now()
	for _, tab_name := range common_tables {
		wg.Add(1)
		go func(tab_name string) {
			envContext := NewEnvContext()
			envContext.SourceDb = Source_db
			envContext.SourceDatabaseName = Config.SourceMySQLDB

			envContext.TargetDb = Target_db
			envContext.TargetDatabaseName = Config.TargetMySQLDB

			envContext.Crc_db = Crc_db
			envContext.CrcDatabaseName = Config.ChecksumDB

			envContext.SourceTableName = tab_name
			envContext.TargetTableName = tab_name
			compare := NewCompare(envContext)
			compare.CompareTable()
		}(tab_name)

	}
	wg.Wait()
	elapsed := time.Since(t)
	time.Sleep(time.Duration(2) * time.Second)
	fmt.Println("table compare elapsed:", elapsed)

	for {
		if Counter == 0 {
			StopMonitoring()
			return
		}
	}

}
