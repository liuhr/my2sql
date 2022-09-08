package base

import (
	"fmt"
	"github.com/siddontang/go-log/log"
	"github.com/go-mysql-org/go-mysql/mysql"
        "github.com/go-mysql-org/go-mysql/replication"
	SQL "my2sql/sqlbuilder"
	toolkits "my2sql/toolkits"
	"strings"
)

var G_Bytes_Column_Types []string = []string{"blob", "json", "geometry", C_unknownColType}

func GetPosStr(name string, spos uint32, epos uint32) string {
	return fmt.Sprintf("%s %d-%d", name, spos, epos)
}

func GetDroppedFieldName(idx int) string {
	return fmt.Sprintf("%s%d", C_unknownColPrefix, idx)
}

func GetAllFieldNamesWithDroppedFields(rowLen int, colNames []FieldInfo) []FieldInfo {
	if rowLen <= len(colNames) {
		return colNames
	}
	var arr []FieldInfo = make([]FieldInfo, rowLen)
	cnt := copy(arr, colNames)
	for i := cnt; i < rowLen; i++ {
		arr[i] = FieldInfo{FieldName: GetDroppedFieldName(i - cnt), FieldType: C_unknownColType}
	}
	return arr
}

func GetSqlFieldsEXpressions(colCnt int, colNames []FieldInfo, tbMap *replication.TableMapEvent) ([]SQL.NonAliasColumn, []string) {
	colDefExps := make([]SQL.NonAliasColumn, colCnt)
	colTypeNames := make([]string, colCnt)
	for i := 0; i < colCnt; i++ {
		typeName, colDef := GetMysqlDataTypeNameAndSqlColumn(colNames[i].FieldType, colNames[i].FieldName, tbMap.ColumnType[i], tbMap.ColumnMeta[i])
		colDefExps[i] = colDef
		colTypeNames[i] = typeName
	}
	return colDefExps, colTypeNames
}

func GetMysqlDataTypeNameAndSqlColumn(tpDef string, colName string, tp byte, meta uint16) (string, SQL.NonAliasColumn) {
	// for unkown type, defaults to BytesColumn

	//get real string type
	if tp == mysql.MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			if b0&0x30 != 0x30 {
				tp = byte(b0 | 0x30)
			} else {
				tp = b0
			}
		}
	}
	//fmt.Println("column type:", colName, tp)
	switch tp {

	case mysql.MYSQL_TYPE_NULL:
		return C_unknownColType, SQL.BytesColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_LONG:
		return "int", SQL.IntColumn(colName, SQL.NotNullable)

	case mysql.MYSQL_TYPE_TINY:
		return "tinyint", SQL.IntColumn(colName, SQL.NotNullable)

	case mysql.MYSQL_TYPE_SHORT:
		return "smallint", SQL.IntColumn(colName, SQL.NotNullable)

	case mysql.MYSQL_TYPE_INT24:
		return "mediumint", SQL.IntColumn(colName, SQL.NotNullable)

	case mysql.MYSQL_TYPE_LONGLONG:
		return "bigint", SQL.IntColumn(colName, SQL.NotNullable)

	case mysql.MYSQL_TYPE_NEWDECIMAL:
		return "decimal", SQL.DoubleColumn(colName, SQL.NotNullable)

	case mysql.MYSQL_TYPE_FLOAT:
		return "float", SQL.DoubleColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_DOUBLE:
		return "double", SQL.DoubleColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_BIT:
		return "bit", SQL.IntColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_TIMESTAMP:
		//return "timestamp", SQL.DateTimeColumn(colName, SQL.NotNullable)
		return "timestamp", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_TIMESTAMP2:
		//return "timestamp", SQL.DateTimeColumn(colName, SQL.NotNullable)
		return "timestamp", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_DATETIME:
		//return "datetime", SQL.DateTimeColumn(colName, SQL.NotNullable)
		return "timestamp", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_DATETIME2:
		//return "datetime", SQL.DateTimeColumn(colName, SQL.NotNullable)
		return "timestamp", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_TIME:
		return "time", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_TIME2:
		return "time", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_DATE:
		return "date", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)

	case mysql.MYSQL_TYPE_YEAR:
		return "year", SQL.IntColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_ENUM:
		return "enum", SQL.IntColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_SET:
		return "set", SQL.IntColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_BLOB:
		//text is stored as blob
		if strings.Contains(strings.ToLower(tpDef), "text") {
			return "blob", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
		}
		return "blob", SQL.BytesColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_VARCHAR,
		mysql.MYSQL_TYPE_VAR_STRING:

		return "varchar", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_STRING:
		return "char", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_JSON:
		//return "json", SQL.BytesColumn(colName, SQL.NotNullable)
		return "json", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)

	case mysql.MYSQL_TYPE_GEOMETRY:
		return "geometry", SQL.BytesColumn(colName, SQL.NotNullable)
	default:
		return C_unknownColType, SQL.BytesColumn(colName, SQL.NotNullable)
	}
}

func GenInsertSqlsForOneRowsEvent(posStr string, rEv *replication.RowsEvent, colDefs []SQL.NonAliasColumn, rowsPerSql int, ifRollback bool, ifprefixDb bool, ifIgnorePrimary bool, primaryIdx []int) []string {
	var (
		insertSql  SQL.InsertStatement
		oneSql     string
		err        error
		i          int
		endIndex   int
		newColDefs []SQL.NonAliasColumn = colDefs[:]
		rowCnt     int                  = len(rEv.Rows)
		schema     string               = string(rEv.Table.Schema)
		table      string               = string(rEv.Table.Table)
		sqlArr     []string
		sqlType    string
	)

	if ifRollback {
		sqlType = "insert_for_delete_rollback"
		ifIgnorePrimary = false
	} else {
		sqlType = "insert"
	}

	if len(primaryIdx) == 0 {
		ifIgnorePrimary = false
	}
	if ifIgnorePrimary {
		newColDefs = GetColDefIgnorePrimary(colDefs, primaryIdx)
	}
	for i = 0; i < rowCnt; i += rowsPerSql {
		insertSql = SQL.NewTable(table, newColDefs...).Insert(newColDefs...)
		endIndex = GetMinValue(rowCnt, i+rowsPerSql)
		oneSql, err = GenInsertSqlForRows(rEv.Rows[i:endIndex], insertSql, schema, ifprefixDb, ifIgnorePrimary, primaryIdx)
		if err != nil {
			log.Fatalf(fmt.Sprintf("Fail to generate %s sql for %s %s \n\terror: %v\n\trows data:%v",
				sqlType, GetAbsTableName(schema, table), posStr, err, rEv.Rows[i:endIndex]))
		} else {
			sqlArr = append(sqlArr, oneSql)
		}

	}

	if endIndex < rowCnt {
		insertSql = SQL.NewTable(table, newColDefs...).Insert(newColDefs...)
		oneSql, err = GenInsertSqlForRows(rEv.Rows[endIndex:rowCnt], insertSql, schema, ifprefixDb, ifIgnorePrimary, primaryIdx)
		if err != nil {
			log.Fatalf(fmt.Sprintf("Fail to generate %s sql for %s %s \n\terror: %s\n\trows data:%v",
				sqlType, GetAbsTableName(schema, table), posStr, err, rEv.Rows[endIndex:rowCnt]))
		} else {
			sqlArr = append(sqlArr, oneSql)
		}
	}
	//fmt.Println("one insert sqlArr", sqlArr)
	return sqlArr

}

func GetColDefIgnorePrimary(colDefs []SQL.NonAliasColumn, primaryIdx []int) []SQL.NonAliasColumn {
	m := []SQL.NonAliasColumn{}
	for i := range colDefs {
		if toolkits.ContainsInt(primaryIdx, i) {
			continue
		}
		m = append(m, colDefs[i])
	}
	return m
}

func ConvertRowToExpressRow(row []interface{}, ifIgnorePrimary bool, primaryIdx []int) []SQL.Expression {

	valueInserted := []SQL.Expression{}
	for i, val := range row {
		if ifIgnorePrimary {
			if toolkits.ContainsInt(primaryIdx, i) {
				continue
			}
		}
		vExp := SQL.Literal(val)
		valueInserted = append(valueInserted, vExp)
	}
	return valueInserted
}

func GenInsertSqlForRows(rows [][]interface{}, insertSql SQL.InsertStatement, schema string, ifprefixDb bool, ifIgnorePrimary bool, primaryIdx []int) (string, error) {

	for _, row := range rows {
		valuesInserted := ConvertRowToExpressRow(row, ifIgnorePrimary, primaryIdx)
		insertSql.Add(valuesInserted...)
	}
	if !ifprefixDb {
		schema = ""
	}

	return insertSql.String(schema)

}

func GenDeleteSqlsForOneRowsEventRollbackInsert(posStr string, rEv *replication.RowsEvent, colDefs []SQL.NonAliasColumn, uniKey []int, ifFullImage bool, ifprefixDb bool) []string {
	return GenDeleteSqlsForOneRowsEvent(posStr, rEv, colDefs, uniKey, ifFullImage, true, ifprefixDb)
}

func GenDeleteSqlsForOneRowsEvent(posStr string, rEv *replication.RowsEvent, colDefs []SQL.NonAliasColumn, uniKey []int, ifFullImage bool, ifRollback bool, ifprefixDb bool) []string {
	rowCnt := len(rEv.Rows)
	sqlArr := make([]string, rowCnt)
	//var sqlArr []string
	schema := string(rEv.Table.Schema)
	table := string(rEv.Table.Table)
	schemaInSql := schema
	if !ifprefixDb {
		schemaInSql = ""
	}

	var sqlType string
	if ifRollback {
		sqlType = "delete_for_insert_rollback"
	} else {
		sqlType = "delete"
	}
	for i, row := range rEv.Rows {
		whereCond := GenEqualConditions(row, colDefs, uniKey, ifFullImage)

		sql, err := SQL.NewTable(table, colDefs...).Delete().Where(SQL.And(whereCond...)).String(schemaInSql)
		if err != nil {
			log.Fatalf(fmt.Sprintf("Fail to generate %s sql for %s %s \n\terror: %s\n\trows data:%v",
				sqlType, GetAbsTableName(schema, table), posStr, err, row))
			//continue
		}
		sqlArr[i] = sql
		//sqlArr = append(sqlArr, sql)
	}
	return sqlArr
}

func GenEqualConditions(row []interface{}, colDefs []SQL.NonAliasColumn, uniKey []int, ifFullImage bool) []SQL.BoolExpression {
	if !ifFullImage && len(uniKey) > 0 {
		expArrs := make([]SQL.BoolExpression, len(uniKey))
		for k, idx := range uniKey {
			expArrs[k] = SQL.EqL(colDefs[idx], row[idx])
		}
		return expArrs
	}
	expArrs := make([]SQL.BoolExpression, len(row))
	for i, v := range row {
		expArrs[i] = SQL.EqL(colDefs[i], v)
	}
	return expArrs
}

func GenInsertSqlsForOneRowsEventRollbackDelete(posStr string, rEv *replication.RowsEvent, colDefs []SQL.NonAliasColumn, rowsPerSql int, ifprefixDb bool) []string {
	return GenInsertSqlsForOneRowsEvent(posStr, rEv, colDefs, rowsPerSql, true, ifprefixDb, false, []int{})
}

func GenUpdateSqlsForOneRowsEvent(posStr string, colsTypeNameFromMysql []string, colsTypeName []string, rEv *replication.RowsEvent, colDefs []SQL.NonAliasColumn, uniKey []int, ifFullImage bool, ifRollback bool, ifprefixDb bool) []string {
	//colsTypeNameFromMysql: for text type, which is stored as blob
	var (
		rowCnt      int    = len(rEv.Rows)
		schema      string = string(rEv.Table.Schema)
		table       string = string(rEv.Table.Table)
		schemaInSql string = schema
		sqlArr      []string
		sql         string
		err         error
		sqlType     string
		wherePart   []SQL.BoolExpression
	)

	if !ifprefixDb {
		schemaInSql = ""
	}

	if ifRollback {
		sqlType = "update_for_update_rollback"
	} else {
		sqlType = "update"
	}
	for i := 0; i < rowCnt; i += 2 {
		upSql := SQL.NewTable(table, colDefs...).Update()
		if ifRollback {
			upSql = GenUpdateSetPart(colsTypeNameFromMysql, colsTypeName, upSql, colDefs, rEv.Rows[i], rEv.Rows[i+1], ifFullImage)
			wherePart = GenEqualConditions(rEv.Rows[i+1], colDefs, uniKey, ifFullImage)
		} else {
			upSql = GenUpdateSetPart(colsTypeNameFromMysql, colsTypeName, upSql, colDefs, rEv.Rows[i+1], rEv.Rows[i], ifFullImage)
			wherePart = GenEqualConditions(rEv.Rows[i], colDefs, uniKey, ifFullImage)
		}

		upSql.Where(SQL.And(wherePart...))
		sql, err = upSql.String(schemaInSql)
		if err != nil {
			log.Fatalf(fmt.Sprintf("Fail to generate %s sql for %s %s \n\terror: %s\n\trows data:%v\n%v",
				sqlType, GetAbsTableName(schema, table), posStr, err, rEv.Rows[i], rEv.Rows[i+1]))
		} else {
			sqlArr = append(sqlArr, sql)
		}

	}
	//fmt.Println(sqlArr)
	return sqlArr

}

func GenUpdateSetPart(colsTypeNameFromMysql []string, colTypeNames []string, updateSql SQL.UpdateStatement, colDefs []SQL.NonAliasColumn, rowAfter []interface{}, rowBefore []interface{}, ifFullImage bool) SQL.UpdateStatement {

	ifUpdateCol := false
	for i, v := range rowAfter {
		ifUpdateCol = false
		//fmt.Printf("type: %s\nbefore: %v\nafter: %v\n", colTypeNames[i], rowBefore[i], v)

		if !ifFullImage {
			// text is stored as blob in binlog
			if toolkits.ContainsString(G_Bytes_Column_Types, colTypeNames[i]) && !strings.Contains(strings.ToLower(colsTypeNameFromMysql[i]), "text") {
				aArr, aOk := v.([]byte)
				bArr, bOk := rowBefore[i].([]byte)
				if aOk && bOk {
					if CompareEquelByteSlice(aArr, bArr) {
						//fmt.Println("bytes compare equal")
						ifUpdateCol = false
					} else {
						ifUpdateCol = true
						//fmt.Println("bytes compare unequal")
					}
				} else {
					//fmt.Println("error to convert to []byte")
					//should update the column
					ifUpdateCol = true
				}

			} else {
				if v == rowBefore[i] {
					//fmt.Println("compare equal")
					ifUpdateCol = false
				} else {
					//fmt.Println("compare unequal")
					ifUpdateCol = true
				}
			}
		} else {
			ifUpdateCol = true
		}

		if ifUpdateCol {
			updateSql.Set(colDefs[i], SQL.Literal(v))
		}
	}
	return updateSql

}
