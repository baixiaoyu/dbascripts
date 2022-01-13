package main

import (
	gosql "database/sql"
	"fmt"
	"sync/atomic"
	"time"
)

type tableWriteFunc func() error

type Compare struct {
	envContext             *EnvContext
	finishedComparing      int64
	sqlQueue               chan tableWriteFunc
	sqlcompareComplete     chan error
	sqlCompareCompleteFlag int64
	//	compareComplete        chan error
}

func NewCompare(context *EnvContext) *Compare {
	compare := &Compare{
		envContext:         context,
		sqlQueue:           make(chan tableWriteFunc),
		sqlcompareComplete: make(chan error),
		//	compareComplete:    make(chan error),
		finishedComparing: 0,
	}
	return compare
}

func (this *Compare) CompareTable() {

	defer wg.Done()

	println("begin compare db", this.envContext.SourceDatabaseName, this.envContext.TargetDatabaseName)

	inspectSourceAndTargetTables(this.envContext)
	ReadMigrationRangeValues("source", this.envContext)
	ReadMigrationRangeValues("target", this.envContext)

	this.envContext.MarkCompareStartTime()

	go this.executeCompareSQL()
	go this.iterateChunks()

	this.consumeCompareComplete()

}

func (this *Compare) consumeCompareComplete() {
	if err := <-this.sqlcompareComplete; err != nil {
		this.envContext.PanicAbort <- err
	}
	atomic.StoreInt64(&this.sqlCompareCompleteFlag, 1)
	this.envContext.MarkCompareEndTime()
	go func() {
		for err := range this.sqlcompareComplete {
			if err != nil {
				this.envContext.PanicAbort <- err
			}
		}
	}()
}

func (this *Compare) executeCompareSQL() error {
	for {
		if atomic.LoadInt64(&this.finishedComparing) > 0 {
			return nil
		}
		select {
		case compareSQLFunc := <-this.sqlQueue:
			{
				if err := compareSQLFunc(); err != nil {
					return err
				}
			}
		default:
			{
				// Hmmmmm... nothing in the queue; no events, but also no row copy.
				// This is possible upon load. Let's just sleep it over.
				println("Getting nothing in the write queue. Sleeping...")
				time.Sleep(time.Second)
			}
		}

	}
	return nil
}

// iterateChunks iterates the existing table rows, and generates a copy task of
// a chunk of rows onto the ghost table.
func (this *Compare) iterateChunks() error {
	terminateRowIteration := func(err error) error {
		this.sqlcompareComplete <- err
		return err
	}

	if this.envContext.SourceRangeMinValues == nil {
		println("No rows found in table. compare will be implicitly empty")
		return terminateRowIteration(nil)
	}

	var hasNoFurtherRangeFlag int64
	// Iterate per chunk:
	for {
		if atomic.LoadInt64(&this.sqlCompareCompleteFlag) == 1 || atomic.LoadInt64(&hasNoFurtherRangeFlag) == 1 {
			// Done
			// There's another such check down the line
			return nil
		}
		compareSQLFunc := func() error {
			if atomic.LoadInt64(&this.sqlCompareCompleteFlag) == 1 || atomic.LoadInt64(&hasNoFurtherRangeFlag) == 1 {
				// Done.
				// There's another such check down the line
				return nil
			}

			hasFurtherRange := false
			var err error
			if hasFurtherRange, err = this.CalculateNextIterationRangeEndValues(); err != nil {
				return terminateRowIteration(err)
			}

			if !hasFurtherRange {
				atomic.StoreInt64(&hasNoFurtherRangeFlag, 1)
				return terminateRowIteration(nil)
			}
			// compare task:
			applyCompareFunc := func() error {
				if atomic.LoadInt64(&this.sqlCompareCompleteFlag) == 1 {
					return nil
				}
				_, rowsAffected, _, err := this.ApplyIterationInsertQuery()
				if err != nil {
					return err // wrapping call will retry
				}
				atomic.AddInt64(&this.envContext.TotalRowsCompared, rowsAffected)
				atomic.AddInt64(&this.envContext.Iteration, 1)
				return nil
			}
			if err := applyCompareFunc(); err != nil {
				return terminateRowIteration(err)
			}
			return nil
		}
		// Enqueue copy operation; to be executed by executeWriteFuncs()
		this.sqlQueue <- compareSQLFunc
	}
	return nil
}

func (this *Compare) CalculateNextIterationRangeEndValues() (hasFurtherRange bool, err error) {
	this.envContext.ComparationIterationRangeMinValues = this.envContext.ComparationIterationRangeMaxValues
	if this.envContext.ComparationIterationRangeMinValues == nil {
		this.envContext.ComparationIterationRangeMinValues = this.envContext.SourceRangeMinValues
	}

	//fmt.Printf("iteration min max:%+v, %+v\n", this.envContext.ComparationIterationRangeMinValues, this.envContext.ComparationIterationRangeMaxValues)
	for i := 0; i < 2; i++ {
		buildFunc := BuildUniqueKeyRangeEndPreparedQueryViaOffset
		if i == 1 {
			buildFunc = BuildUniqueKeyRangeEndPreparedQueryViaTemptable
		}
		query, explodedArgs, err := buildFunc(
			this.envContext.SourceDatabaseName,
			this.envContext.SourceTableName,
			&this.envContext.UniqueKey.Columns,
			this.envContext.ComparationIterationRangeMinValues.AbstractValues(),
			this.envContext.SourceRangeMaxValues.AbstractValues(),
			atomic.LoadInt64(&this.envContext.ChunkSize),
			this.envContext.GetIteration() == 0,
			fmt.Sprintf("iteration:%d", this.envContext.GetIteration()),
		)

		if err != nil {
			return hasFurtherRange, err
		}

		rows, err := this.envContext.SourceDb.Query(query, explodedArgs...)
		if err != nil {
			return hasFurtherRange, err
		}
		iterationRangeMaxValues := NewColumnValues(this.envContext.UniqueKey.Len())

		for rows.Next() {
			if err = rows.Scan(iterationRangeMaxValues.ValuesPointers...); err != nil {

				return hasFurtherRange, err
			}
			hasFurtherRange = true
		}
		if err = rows.Err(); err != nil {
			return hasFurtherRange, err
		}
		if hasFurtherRange {
			this.envContext.ComparationIterationRangeMaxValues = iterationRangeMaxValues
			return hasFurtherRange, nil
		}
	}
	fmt.Println("Iteration complete: no further range to iterate")
	return hasFurtherRange, nil
}

type Result struct {
	cnt int
	crc string
}

func (this *Compare) ApplyIterationInsertQuery() (chunkSize int64, rowsAffected int64, duration time.Duration, err error) {
	startTime := time.Now()
	chunkSize = atomic.LoadInt64(&this.envContext.ChunkSize)

	query, explodedArgs, err := BuildRangeInsertPreparedQuery(
		this.envContext.SourceTableName,
		this.envContext.SharedColumns.Names(),
		this.envContext.MappedSharedColumns.Names(),
		this.envContext.SourceNullableColumns.Names(),
		this.envContext.UniqueKey.Name,
		&this.envContext.UniqueKey.Columns,
		this.envContext.ComparationIterationRangeMinValues.AbstractValues(),
		this.envContext.ComparationIterationRangeMaxValues.AbstractValues(),
		this.envContext.GetIteration() == 0,
		true,
	)
	if err != nil {
		return chunkSize, rowsAffected, duration, err
	}
	//fmt.Printf("insert sql%+v\n", query)
	// fmt.Printf("args %+v\n", explodedArgs)

	// execute compare sql on source and target db
	SourceRows, err := this.envContext.SourceDb.Query(query, explodedArgs...)
	var sourceCnt int
	var sourceCrc string
	for SourceRows.Next() {
		if err = SourceRows.Scan(&sourceCnt, &sourceCrc); err != nil {
			fmt.Printf("get crc error %+v\n", err)
			this.envContext.PanicAbort <- err
		}
	}

	TargetRows, err := this.envContext.TargetDb.Query(query, explodedArgs...)
	var targetCnt int
	var targetCrc string
	for TargetRows.Next() {
		if err = TargetRows.Scan(&targetCnt, &targetCrc); err != nil {
			fmt.Printf("get crc error %+v\n", err)
			this.envContext.PanicAbort <- err
		}
	}

	query = fmt.Sprintf(`insert into crc_result(dbname,table_name,chunk_num,start_pk,end_pk,crc,cnt,ctime,which) 
	values('%s','%s','%d','%s','%s','%s','%d',now(),'source'),('%s','%s','%d','%s','%s','%s','%d',now(),'target')`,
		this.envContext.SourceDatabaseName, this.envContext.SourceTableName, this.envContext.GetIteration(),
		this.envContext.ComparationIterationRangeMinValues.String(),
		this.envContext.ComparationIterationRangeMaxValues.String(), sourceCrc, sourceCnt,
		this.envContext.TargetDatabaseName, this.envContext.TargetTableName, this.envContext.GetIteration(),
		this.envContext.ComparationIterationRangeMinValues.String(),
		this.envContext.ComparationIterationRangeMaxValues.String(), targetCrc, targetCnt)

	//fmt.Printf("crc query is:%+v\n", query)
	sqlResult, err := func() (gosql.Result, error) {
		tx, err := this.envContext.Crc_db.Begin()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()

		result, err := tx.Exec(query)

		if err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return result, nil
	}()

	if err != nil {
		return chunkSize, rowsAffected, duration, err
	}
	rowsAffected, _ = sqlResult.RowsAffected()
	duration = time.Since(startTime)

	return chunkSize, rowsAffected, duration, nil
}
