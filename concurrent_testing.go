package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var db *sqlx.DB
var wg sync.WaitGroup

func initMySQL() (err error) {
	dsn := "msandbox:msandbox@tcp(127.0.0.1:20996)/db1"
	db, err = sqlx.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("connect server failed, err:%v\n", err)
		return
	}
	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(10)
	return
}

func updateRowDemo() {
	rand.Seed(time.Now().Unix())
	id := rand.Intn(5000000)
	sqlStr := "update sbtest1 set utime=now() where id = ?"

	_, err := db.Exec(sqlStr, id)
	if err != nil {
		fmt.Printf("update failed, err:%v\n", err)
		return
	}
	// n, err := ret.RowsAffected() // 操作影响的行数
	// if err != nil {
	// 	fmt.Printf("get RowsAffected failed, err:%v\n", err)
	// 	return
	// }
	//fmt.Printf("update success, affected rows:%d\n", n)
	wg.Done()

}

func prepareQueryDemo(starttime string, endtime string) {
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

func main() {

	initMySQL()
	defer db.Close()
	t := time.Now()
	for i := 0; i < 120; i++ {
		wg.Add(1)
		go updateRowDemo()
	}
	wg.Wait()
	elapsed := time.Since(t)
	fmt.Println("app elapsed:", elapsed)

}
