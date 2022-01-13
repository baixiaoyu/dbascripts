package main

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gosql "database/sql"

	"github.com/openark/golib/log"
	"github.com/openark/golib/sqlutils"
)

type EnvContext struct {
	SourceDb                           *gosql.DB
	SourceDatabaseName                 string
	SourceTableName                    string
	SourceTableColumns                 *ColumnList
	SourceVirtualColumns               *ColumnList
	SourceNullableColumns              *ColumnList
	SourceUniqueKeys                   [](*UniqueKey)
	SourceRangeMinValues               *ColumnValues
	SourceRangeMaxValues               *ColumnValues
	ComparationIterationRangeMinValues *ColumnValues
	ComparationIterationRangeMaxValues *ColumnValues
	TargetDb                           *gosql.DB
	TargetDatabaseName                 string
	TargetTableName                    string
	TargetTableColumns                 *ColumnList
	TargetVirtualColumns               *ColumnList
	TargetNullableColumns              *ColumnList
	TargetUniqueKeys                   [](*UniqueKey)
	TargetRangeMinValues               *ColumnValues
	TargetRangeMaxValues               *ColumnValues
	UniqueKey                          *UniqueKey
	SharedColumns                      *ColumnList
	MappedSharedColumns                *ColumnList
	SQLCompareStartTime                time.Time
	SQLCompareEndTime                  time.Time
	PanicAbort                         chan error
	Iteration                          int64
	TotalRowsCompared                  int64
	ChunkSize                          int64
	Crc_db                             *gosql.DB
	CrcDatabaseName                    string
}

func NewEnvContext() *EnvContext {
	return &EnvContext{
		ChunkSize: 1000,
	}
}

func (this *EnvContext) GetIteration() int64 {
	return atomic.LoadInt64(&this.Iteration)
}

// MarkRowCopyStartTime
func (this *EnvContext) MarkCompareStartTime() {
	this.SQLCompareStartTime = time.Now()
}

// ElapsedRowCopyTime returns time since starting to copy chunks of rows
func (this *EnvContext) ElapsedComppareTime() time.Duration {

	if this.SQLCompareStartTime.IsZero() {
		// Row copy hasn't started yet
		return 0
	}

	if this.SQLCompareEndTime.IsZero() {
		return time.Since(this.SQLCompareStartTime)
	}
	return this.SQLCompareEndTime.Sub(this.SQLCompareStartTime)
}

// ElapsedRowCopyTime returns time since starting to copy chunks of rows
func (this *EnvContext) MarkCompareEndTime() {
	this.SQLCompareEndTime = time.Now()
}

// getSharedUniqueKeys returns the intersection of two given unique keys,
// testing by list of columns
func getSharedUniqueKeys(sourceUniqueKeys, targetUniqueKeys [](*UniqueKey)) (uniqueKeys [](*UniqueKey), err error) {
	// We actually do NOT rely on key name, just on the set of columns. This is because maybe
	// the ALTER is on the name itself...
	for _, sourceUniqueKey := range sourceUniqueKeys {
		for _, targetUniqueKey := range targetUniqueKeys {
			if sourceUniqueKey.Columns.EqualsByNames(&targetUniqueKey.Columns) {
				uniqueKeys = append(uniqueKeys, sourceUniqueKey)
			}
		}
	}
	return uniqueKeys, nil
}

func EscapeName(name string) string {
	if unquoted, err := strconv.Unquote(name); err == nil {
		name = unquoted
	}
	return fmt.Sprintf("`%s`", name)
}

func GetTableColumns(db *gosql.DB, databaseName, tableName string) (*ColumnList, *ColumnList, *ColumnList, error) {
	query := fmt.Sprintf(`
		show columns from %s.%s
		`,
		EscapeName(databaseName),
		EscapeName(tableName),
	)
	columnNames := []string{}
	virtualColumnNames := []string{}
	nullableColumnNames := []string{}
	err := sqlutils.QueryRowsMap(db, query, func(rowMap sqlutils.RowMap) error {
		columnName := rowMap.GetString("Field")
		columnNames = append(columnNames, columnName)
		if strings.Contains(rowMap.GetString("Extra"), "GENERATED") {
			virtualColumnNames = append(virtualColumnNames, columnName)
		}
		if strings.Contains(rowMap.GetString("Null"), "YES") {
			nullableColumnNames = append(nullableColumnNames, columnName)
		}
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if len(columnNames) == 0 {
		return nil, nil, nil, log.Errorf("Found 0 columns on %s.%s. Bailing out",
			EscapeName(databaseName),
			EscapeName(tableName),
		)
	}
	return NewColumnList(columnNames), NewColumnList(virtualColumnNames), NewColumnList(nullableColumnNames), nil
}

func InspectTableColumnsAndUniqueKeys(db *gosql.DB, databaseName string, tableName string) (columns *ColumnList, virtualColumns *ColumnList, nullableColumns *ColumnList, uniqueKeys [](*UniqueKey), err error) {
	uniqueKeys, err = getCandidateUniqueKeys(db, databaseName, tableName)
	if err != nil {
		return columns, virtualColumns, nullableColumns, uniqueKeys, err
	}
	if len(uniqueKeys) == 0 {
		return columns, virtualColumns, nullableColumns, uniqueKeys, fmt.Errorf("No PRIMARY nor UNIQUE key found in table! Bailing out")
	}
	columns, virtualColumns, nullableColumns, err = GetTableColumns(db, databaseName, tableName)
	if err != nil {
		return columns, virtualColumns, nullableColumns, uniqueKeys, err
	}

	return columns, virtualColumns, nullableColumns, uniqueKeys, nil
}

func inspectSourceAndTargetTables(envContext *EnvContext) (err error) {
	println("begin inspect")
	envContext.SourceTableColumns, envContext.SourceVirtualColumns, envContext.SourceNullableColumns, envContext.SourceUniqueKeys, err = InspectTableColumnsAndUniqueKeys(envContext.SourceDb, envContext.SourceDatabaseName, envContext.SourceTableName)
	if err != nil {
		return err
	}

	envContext.TargetTableColumns, envContext.TargetVirtualColumns, envContext.TargetNullableColumns, envContext.TargetUniqueKeys, err = InspectTableColumnsAndUniqueKeys(envContext.TargetDb, envContext.TargetDatabaseName, envContext.TargetTableName)
	if err != nil {
		return err
	}

	sourceNames := envContext.SourceTableColumns.Names()
	TargetNames := envContext.TargetTableColumns.Names()

	if !reflect.DeepEqual(sourceNames, TargetNames) {
		//todo 添加一个开关，可以控制是否允许列不同
		println("It seems like table structure is not identical between source and target.")
		envContext.PanicAbort <- err
	}

	sharedUniqueKeys, err := getSharedUniqueKeys(envContext.SourceUniqueKeys, envContext.TargetUniqueKeys)
	if err != nil {
		return err
	}

	for i, sharedUniqueKey := range sharedUniqueKeys {
		applyColumnTypes(*envContext, envContext.SourceDatabaseName, envContext.SourceTableName, &sharedUniqueKey.Columns)
		uniqueKeyIsValid := true
		for _, column := range sharedUniqueKey.Columns.Columns() {
			switch column.Type {
			case FloatColumnType:
				{
					uniqueKeyIsValid = false
				}
			case JSONColumnType:
				{
					// Noteworthy that at this time MySQL does not allow JSON indexing anyhow, but this code
					// will remain in place to potentially handle the future case where JSON is supported in indexes.
					uniqueKeyIsValid = false
				}
			}
		}
		if uniqueKeyIsValid {
			envContext.UniqueKey = sharedUniqueKeys[i]
			break
		}
	}

	if envContext.UniqueKey == nil {
		return fmt.Errorf("No shared unique key can be found after!")
	}
	// fmt.Printf("Chosen shared unique key is %s\n", envContext.UniqueKey.Name)
	// fmt.Printf("uniqu key columns:%+v\n", envContext.UniqueKey)
	if envContext.UniqueKey.HasNullable {

		fmt.Printf("Chosen key (%s) has nullable columns.", envContext.UniqueKey)

	}

	envContext.SharedColumns, envContext.MappedSharedColumns = getSharedColumns(envContext.SourceTableColumns, envContext.TargetTableColumns, envContext.SourceVirtualColumns, envContext.TargetVirtualColumns)

	return nil
}

func BuildUniqueKeyRangeEndPreparedQueryViaTemptable(databaseName, tableName string, uniqueKeyColumns *ColumnList, rangeStartArgs, rangeEndArgs []interface{}, chunkSize int64, includeRangeStartValues bool, hint string) (result string, explodedArgs []interface{}, err error) {
	if uniqueKeyColumns.Len() == 0 {
		return "", explodedArgs, fmt.Errorf("Got 0 columns in BuildUniqueKeyRangeEndPreparedQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	var startRangeComparisonSign ValueComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		startRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}
	rangeStartComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeStartArgs, startRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	rangeEndComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
	uniqueKeyColumnDescending := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
	for i, column := range uniqueKeyColumns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == EnumColumnType {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("concat(%s) asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("concat(%s) desc", uniqueKeyColumnNames[i])
		} else {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("%s desc", uniqueKeyColumnNames[i])
		}
	}
	result = fmt.Sprintf(`
      select /* gh-ost %s.%s %s */ %s
				from (
					select
							%s
						from
							%s.%s
						where %s and %s
						order by
							%s
						limit %d
				) select_osc_chunk
			order by
				%s
			limit 1
    `, databaseName, tableName, hint, strings.Join(uniqueKeyColumnNames, ", "),
		strings.Join(uniqueKeyColumnNames, ", "), databaseName, tableName,
		rangeStartComparison, rangeEndComparison,
		strings.Join(uniqueKeyColumnAscending, ", "), chunkSize,
		strings.Join(uniqueKeyColumnDescending, ", "),
	)
	return result, explodedArgs, nil
}

func getSharedColumns(originalColumns, ghostColumns *ColumnList, originalVirtualColumns, ghostVirtualColumns *ColumnList) (*ColumnList, *ColumnList) {
	sharedColumnNames := []string{}
	for _, originalColumn := range originalColumns.Names() {
		isSharedColumn := false
		for _, ghostColumn := range ghostColumns.Names() {
			if strings.EqualFold(originalColumn, ghostColumn) {
				isSharedColumn = true
				break
			}
		}

		for _, virtualColumn := range originalVirtualColumns.Names() {
			if strings.EqualFold(originalColumn, virtualColumn) {
				isSharedColumn = false
			}
		}
		for _, virtualColumn := range ghostVirtualColumns.Names() {
			if strings.EqualFold(originalColumn, virtualColumn) {
				isSharedColumn = false
			}
		}
		if isSharedColumn {
			sharedColumnNames = append(sharedColumnNames, originalColumn)
		}
	}
	mappedSharedColumnNames := []string{}
	for _, columnName := range sharedColumnNames {
		mappedSharedColumnNames = append(mappedSharedColumnNames, columnName)
	}
	return NewColumnList(sharedColumnNames), NewColumnList(mappedSharedColumnNames)
}

// applyColumnTypes
func applyColumnTypes(envContext EnvContext, databaseName, tableName string, columnsLists ...*ColumnList) error {
	query := `
		select
				*
			from
				information_schema.columns
			where
				table_schema=?
				and table_name=?
		`
	err := sqlutils.QueryRowsMap(envContext.SourceDb, query, func(m sqlutils.RowMap) error {
		columnName := m.GetString("COLUMN_NAME")
		columnType := m.GetString("COLUMN_TYPE")
		columnOctetLength := m.GetUint("CHARACTER_OCTET_LENGTH")
		for _, columnsList := range columnsLists {
			column := columnsList.GetColumn(columnName)
			if column == nil {
				continue
			}

			if strings.Contains(columnType, "unsigned") {
				column.IsUnsigned = true
			}
			if strings.Contains(columnType, "mediumint") {
				column.Type = MediumIntColumnType
			}
			if strings.Contains(columnType, "timestamp") {
				column.Type = TimestampColumnType
			}
			if strings.Contains(columnType, "datetime") {
				column.Type = DateTimeColumnType
			}
			if strings.Contains(columnType, "json") {
				column.Type = JSONColumnType
			}
			if strings.Contains(columnType, "float") {
				column.Type = FloatColumnType
			}
			if strings.HasPrefix(columnType, "enum") {
				column.Type = EnumColumnType
			}
			if strings.HasPrefix(columnType, "binary") {
				column.Type = BinaryColumnType
				column.BinaryOctetLength = columnOctetLength
			}
			if charset := m.GetString("CHARACTER_SET_NAME"); charset != "" {
				column.Charset = charset
			}
		}
		return nil
	}, databaseName, tableName)
	return err
}

func ReadMigrationRangeValues(env string, envContext *EnvContext) error {
	if err := ReadMigrationMinValues(envContext, env); err != nil {
		return err
	}
	if err := ReadMigrationMaxValues(envContext, env); err != nil {
		return err
	}
	return nil
}

// ReadMigrationMinValues returns the minimum values to be iterated on rowcopy
func ReadMigrationMinValues(envContext *EnvContext, env string) error {
	//this.migrationContext.Log.Debugf("Reading migration range according to key: %s", uniqueKey.Name)
	var databaesName, tableName string
	var db *gosql.DB
	if env == "source" {
		databaesName = envContext.SourceDatabaseName
		tableName = envContext.SourceTableName
		db = envContext.SourceDb
	}
	if env == "target" {
		databaesName = envContext.TargetDatabaseName
		tableName = envContext.TargetTableName
		db = envContext.TargetDb
	}

	query, err := BuildUniqueKeyMinValuesPreparedQuery(databaesName, tableName, &envContext.UniqueKey.Columns)
	if err != nil {
		return err
	}
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	if env == "source" {
		for rows.Next() {
			envContext.SourceRangeMinValues = NewColumnValues(envContext.UniqueKey.Len())
			if err = rows.Scan(envContext.SourceRangeMinValues.ValuesPointers...); err != nil {
				return err
			}
		}
	} else {
		for rows.Next() {
			envContext.TargetRangeMinValues = NewColumnValues(envContext.UniqueKey.Len())
			if err = rows.Scan(envContext.TargetRangeMinValues.ValuesPointers...); err != nil {
				return err
			}
		}
	}

	err = rows.Err()
	return err
}

// ReadMigrationMaxValues returns the maximum values to be iterated on rowcopy
func ReadMigrationMaxValues(envContext *EnvContext, env string) error {
	var databaesName, tableName string
	var db *gosql.DB
	//println("get max value", env)
	if env == "source" {
		databaesName = envContext.SourceDatabaseName
		tableName = envContext.SourceTableName
		db = envContext.SourceDb
	}
	if env == "target" {
		databaesName = envContext.TargetDatabaseName
		tableName = envContext.TargetTableName
		db = envContext.TargetDb
	}

	query, err := BuildUniqueKeyMaxValuesPreparedQuery(databaesName, tableName, &envContext.UniqueKey.Columns)
	if err != nil {
		return err
	}
	//println("get max query", query)
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	if env == "source" {
		for rows.Next() {
			envContext.SourceRangeMaxValues = NewColumnValues(envContext.UniqueKey.Len())
			if err = rows.Scan(envContext.SourceRangeMaxValues.ValuesPointers...); err != nil {
				return err
			}
		}
	} else {
		for rows.Next() {
			envContext.TargetRangeMaxValues = NewColumnValues(envContext.UniqueKey.Len())
			if err = rows.Scan(envContext.TargetRangeMaxValues.ValuesPointers...); err != nil {
				return err
			}
		}
	}
	err = rows.Err()
	return err
}

func getCandidateUniqueKeys(db *gosql.DB, databaseName string, tableName string) (uniqueKeys [](*UniqueKey), err error) {
	query := `
    SELECT
      COLUMNS.TABLE_SCHEMA,
      COLUMNS.TABLE_NAME,
      COLUMNS.COLUMN_NAME,
      UNIQUES.INDEX_NAME,
      UNIQUES.COLUMN_NAMES,
      UNIQUES.COUNT_COLUMN_IN_INDEX,
      COLUMNS.DATA_TYPE,
      COLUMNS.CHARACTER_SET_NAME,
			LOCATE('auto_increment', EXTRA) > 0 as is_auto_increment,
      has_nullable
    FROM INFORMATION_SCHEMA.COLUMNS INNER JOIN (
      SELECT
        TABLE_SCHEMA,
        TABLE_NAME,
        INDEX_NAME,
        COUNT(*) AS COUNT_COLUMN_IN_INDEX,
        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
        SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME,
        SUM(NULLABLE='YES') > 0 AS has_nullable
      FROM INFORMATION_SCHEMA.STATISTICS
      WHERE
				NON_UNIQUE=0
				AND TABLE_SCHEMA = ?
      	AND TABLE_NAME = ?
      GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
    ) AS UNIQUES
    ON (
      COLUMNS.COLUMN_NAME = UNIQUES.FIRST_COLUMN_NAME
    )
    WHERE
      COLUMNS.TABLE_SCHEMA = ?
      AND COLUMNS.TABLE_NAME = ?
    ORDER BY
      COLUMNS.TABLE_SCHEMA, COLUMNS.TABLE_NAME,
      CASE UNIQUES.INDEX_NAME
        WHEN 'PRIMARY' THEN 0
        ELSE 1
      END,
      CASE has_nullable
        WHEN 0 THEN 0
        ELSE 1
      END,
      CASE IFNULL(CHARACTER_SET_NAME, '')
          WHEN '' THEN 0
          ELSE 1
      END,
      CASE DATA_TYPE
        WHEN 'tinyint' THEN 0
        WHEN 'smallint' THEN 1
        WHEN 'int' THEN 2
        WHEN 'bigint' THEN 3
        ELSE 100
      END,
      COUNT_COLUMN_IN_INDEX
  `
	err = sqlutils.QueryRowsMap(db, query, func(m sqlutils.RowMap) error {
		uniqueKey := &UniqueKey{
			Name:            m.GetString("INDEX_NAME"),
			Columns:         *ParseColumnList(m.GetString("COLUMN_NAMES")),
			HasNullable:     m.GetBool("has_nullable"),
			IsAutoIncrement: m.GetBool("is_auto_increment"),
		}
		uniqueKeys = append(uniqueKeys, uniqueKey)
		return nil
	}, databaseName, tableName, databaseName, tableName)
	if err != nil {
		return uniqueKeys, err
	}
	return uniqueKeys, nil
}

type ValueComparisonSign string

const (
	LessThanComparisonSign            ValueComparisonSign = "<"
	LessThanOrEqualsComparisonSign    ValueComparisonSign = "<="
	EqualsComparisonSign              ValueComparisonSign = "="
	GreaterThanOrEqualsComparisonSign ValueComparisonSign = ">="
	GreaterThanComparisonSign         ValueComparisonSign = ">"
	NotEqualsComparisonSign           ValueComparisonSign = "!="
)

func buildColumnsPreparedValues(columns *ColumnList) []string {
	values := make([]string, columns.Len())
	for i, column := range columns.Columns() {
		var token string
		if column.timezoneConversion != nil {
			token = fmt.Sprintf("convert_tz(?, '%s', '%s')", column.timezoneConversion.ToTimezone, "+00:00")
		} else if column.Type == JSONColumnType {
			token = "convert(? using utf8mb4)"
		} else {
			token = "?"
		}
		values[i] = token
	}
	return values
}

func BuildValueComparison(column string, value string, comparisonSign ValueComparisonSign) (result string, err error) {
	if column == "" {
		return "", fmt.Errorf("Empty column in GetValueComparison")
	}
	if value == "" {
		return "", fmt.Errorf("Empty value in GetValueComparison")
	}
	comparison := fmt.Sprintf("(%s %s %s)", EscapeName(column), string(comparisonSign), value)
	return comparison, err
}

func BuildEqualsComparison(columns []string, values []string) (result string, err error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("Got 0 columns in GetEqualsComparison")
	}
	if len(columns) != len(values) {
		return "", fmt.Errorf("Got %d columns but %d values in GetEqualsComparison", len(columns), len(values))
	}
	comparisons := []string{}
	for i, column := range columns {
		value := values[i]
		comparison, err := BuildValueComparison(column, value, EqualsComparisonSign)
		if err != nil {
			return "", err
		}
		comparisons = append(comparisons, comparison)
	}
	result = strings.Join(comparisons, " and ")
	result = fmt.Sprintf("(%s)", result)
	return result, nil
}

func BuildRangeComparison(columns []string, values []string, args []interface{}, comparisonSign ValueComparisonSign) (result string, explodedArgs []interface{}, err error) {
	if len(columns) == 0 {
		return "", explodedArgs, fmt.Errorf("Got 0 columns in GetRangeComparison")
	}
	if len(columns) != len(values) {
		return "", explodedArgs, fmt.Errorf("Got %d columns but %d values in GetEqualsComparison", len(columns), len(values))
	}
	if len(columns) != len(args) {
		return "", explodedArgs, fmt.Errorf("Got %d columns but %d args in GetEqualsComparison", len(columns), len(args))
	}
	includeEquals := false
	if comparisonSign == LessThanOrEqualsComparisonSign {
		comparisonSign = LessThanComparisonSign
		includeEquals = true
	}
	if comparisonSign == GreaterThanOrEqualsComparisonSign {
		comparisonSign = GreaterThanComparisonSign
		includeEquals = true
	}
	comparisons := []string{}

	for i, column := range columns {
		value := values[i]
		rangeComparison, err := BuildValueComparison(column, value, comparisonSign)
		if err != nil {
			return "", explodedArgs, err
		}
		if i > 0 {
			equalitiesComparison, err := BuildEqualsComparison(columns[0:i], values[0:i])
			if err != nil {
				return "", explodedArgs, err
			}
			comparison := fmt.Sprintf("(%s AND %s)", equalitiesComparison, rangeComparison)
			comparisons = append(comparisons, comparison)
			explodedArgs = append(explodedArgs, args[0:i]...)
			explodedArgs = append(explodedArgs, args[i])
		} else {
			comparisons = append(comparisons, rangeComparison)
			explodedArgs = append(explodedArgs, args[i])
		}
	}

	if includeEquals {
		comparison, err := BuildEqualsComparison(columns, values)
		if err != nil {
			return "", explodedArgs, nil
		}
		comparisons = append(comparisons, comparison)
		explodedArgs = append(explodedArgs, args...)
	}
	result = strings.Join(comparisons, " or ")
	result = fmt.Sprintf("(%s)", result)
	return result, explodedArgs, nil
}

func BuildRangePreparedComparison(columns *ColumnList, args []interface{}, comparisonSign ValueComparisonSign) (result string, explodedArgs []interface{}, err error) {
	values := buildColumnsPreparedValues(columns)
	return BuildRangeComparison(columns.Names(), values, args, comparisonSign)
}

func BuildUniqueKeyRangeEndPreparedQueryViaOffset(databaseName, tableName string, uniqueKeyColumns *ColumnList, rangeStartArgs, rangeEndArgs []interface{}, chunkSize int64, includeRangeStartValues bool, hint string) (result string, explodedArgs []interface{}, err error) {
	if uniqueKeyColumns.Len() == 0 {
		return "", explodedArgs, fmt.Errorf("Got 0 columns in BuildUniqueKeyRangeEndPreparedQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	var startRangeComparisonSign ValueComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		startRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}
	rangeStartComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeStartArgs, startRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	rangeEndComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
	uniqueKeyColumnDescending := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
	for i, column := range uniqueKeyColumns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == EnumColumnType {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("concat(%s) asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("concat(%s) desc", uniqueKeyColumnNames[i])
		} else {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("%s desc", uniqueKeyColumnNames[i])
		}
	}
	result = fmt.Sprintf(`
				select  /* gh-ost %s.%s %s */
						%s
					from
						%s.%s
					where %s and %s
					order by
						%s
					limit 1
					offset %d
    `, databaseName, tableName, hint,
		strings.Join(uniqueKeyColumnNames, ", "),
		databaseName, tableName,
		rangeStartComparison, rangeEndComparison,
		strings.Join(uniqueKeyColumnAscending, ", "),
		(chunkSize - 1),
	)
	return result, explodedArgs, nil
}

func BuildRangeInsertPreparedQuery(originalTableName string, sharedColumns []string, mappedSharedColumns []string, nullableColumns []string, uniqueKey string, uniqueKeyColumns *ColumnList, rangeStartArgs, rangeEndArgs []interface{}, includeRangeStartValues bool, transactionalTable bool) (result string, explodedArgs []interface{}, err error) {
	rangeStartValues := buildColumnsPreparedValues(uniqueKeyColumns)
	rangeEndValues := buildColumnsPreparedValues(uniqueKeyColumns)
	return BuildRangeInsertQuery(originalTableName, sharedColumns, mappedSharedColumns, nullableColumns, uniqueKey, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, includeRangeStartValues, transactionalTable)
}

func BuildRangeInsertQuery(originalTableName string, sharedColumns []string, mappedSharedColumns []string, nullableColumns []string, uniqueKey string, uniqueKeyColumns *ColumnList, rangeStartValues, rangeEndValues []string, rangeStartArgs, rangeEndArgs []interface{}, includeRangeStartValues bool, transactionalTable bool) (result string, explodedArgs []interface{}, err error) {
	if len(sharedColumns) == 0 {
		return "", explodedArgs, fmt.Errorf("Got 0 shared columns in BuildRangeInsertQuery")
	}

	originalTableName = EscapeName(originalTableName)

	sharedColumns = duplicateNames(sharedColumns)
	for i := range sharedColumns {
		sharedColumns[i] = EscapeName(sharedColumns[i])
	}
	sharedColumnsListing := strings.Join(sharedColumns, ", ")

	nullableColumnsListing := strings.Join(nullableColumns, ", ")
	uniqueKey = EscapeName(uniqueKey)
	var minRangeComparisonSign ValueComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		minRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}
	rangeStartComparison, rangeExplodedArgs, err := BuildRangeComparison(uniqueKeyColumns.Names(), rangeStartValues, rangeStartArgs, minRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	rangeEndComparison, rangeExplodedArgs, err := BuildRangeComparison(uniqueKeyColumns.Names(), rangeEndValues, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	result = fmt.Sprintf(`select count(*) as cnt,
	COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s,CONCAT(%s))) AS UNSIGNED)), 10, 16)), 0) AS crc 
	from %s force index(%s) where (%s and %s)`,
		sharedColumnsListing, nullableColumnsListing,
		originalTableName, uniqueKey,
		rangeStartComparison, rangeEndComparison)

	return result, explodedArgs, nil
}
