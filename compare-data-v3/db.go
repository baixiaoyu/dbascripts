package main

import (
	gosql "database/sql"
	"fmt"
	"net"

	_ "github.com/go-sql-driver/mysql"
	"github.com/openark/golib/sqlutils"
)

type InstanceKey struct {
	Hostname string
	Port     int
}

type ConnectionConfig struct {
	Key      InstanceKey
	User     string
	Password string

	Timeout float64
}

func NewConnectionConfig() *ConnectionConfig {
	config := &ConnectionConfig{
		Key: InstanceKey{},
	}

	return config
}

func (this *ConnectionConfig) GetDBUri(databaseName string) string {
	hostname := this.Key.Hostname
	var ip = net.ParseIP(hostname)
	if (ip != nil) && (ip.To4() == nil) {
		// Wrap IPv6 literals in square brackets
		hostname = fmt.Sprintf("[%s]", hostname)
	}
	interpolateParams := true
	// go-mysql-driver defaults to false if tls param is not provided; explicitly setting here to
	// simplify construction of the DSN below.
	tlsOption := "false"

	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=%fs&readTimeout=%fs&writeTimeout=%fs&interpolateParams=%t&autocommit=true&charset=utf8mb4,utf8,latin1&tls=%s", this.User, this.Password, hostname, this.Key.Port, databaseName, this.Timeout, this.Timeout, this.Timeout, interpolateParams, tlsOption)
}

func GetDB(connectionConfig *ConnectionConfig, DatabaseName string) (*gosql.DB, error) {
	mysql_uri := connectionConfig.GetDBUri(DatabaseName)
	db, err := gosql.Open("mysql", mysql_uri)
	if err != nil {
		fmt.Println(err)
	}
	return db, err

}

func GetTables(db *gosql.DB, dbname string) (tables []string, err error) {
	table_list := []string{}
	cName := fmt.Sprintf("Tables_in_%s", dbname)
	err = sqlutils.QueryRowsMap(db, `show tables`, func(m sqlutils.RowMap) error {
		table_name := m.GetString(cName)
		table_list = append(table_list, table_name)
		return nil
	})
	return table_list, err
}

func SetISOLATION(db *gosql.DB) {
	sql := "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"
	_, err := db.Exec(sql)
	if err != nil {
		fmt.Printf("set session ISOLATION err:%+v\n", err)
	}
}
