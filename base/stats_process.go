package base

import (
	//"os"
	"fmt"
	"sync"
	"strings"
	//"path/filepath"
	"my2sql/dsql"
	constvar "my2sql/constvar"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/replication"
)

var (
	//gDdlRegexp *regexp.Regexp = regexp.MustCompile(C_ddlRegexp)
	Stats_Result_Header_Column_names []string = []string{"binlog", "starttime", "stoptime",
		"startpos", "stoppos", "inserts", "updates", "deletes", "database", "table"}
	Stats_DDL_Header_Column_names        []string = []string{"datetime", "binlog", "startpos", "stoppos", "sql"}
	Stats_BigLongTrx_Header_Column_names []string = []string{"binlog", "starttime", "stoptime", "startpos", "stoppos", "rows", "duration", "tables"}
)

type BinEventStats struct {
	Timestamp     uint32
	Binlog        string
	StartPos      uint32
	StopPos       uint32
	Database      string
	Table         string
	QueryType     string // query, insert, update, delete
	RowCnt        uint32
	QuerySql      string        // for type=query
	ParsedSqlInfo *dsql.SqlInfo // for ddl
}

type OrgSqlPrint struct {
	Binlog   string
	StartPos uint32
	StopPos  uint32
	DateTime uint32
	QuerySql string
}

type BinEventStatsPrint struct {
	Binlog    string
	StartTime uint32
	StopTime  uint32
	StartPos  uint32
	StopPos   uint32
	Database  string
	Table     string
	Inserts   uint32
	Updates   uint32
	Deletes   uint32
}

type BigLongTrxInfo struct {
	//IsBig bool
	//IsLong bool
	StartTime  uint32
	StopTime   uint32
	Binlog     string
	StartPos   uint32
	StopPos    uint32
	RowCnt     uint32                       // total row count for all statement
	Duration   uint32                       // how long the trx lasts
	Statements map[string]map[string]uint32 // rowcnt for each type statment: insert, update, delete. {db1.tb1:{insert:0, update:2, delete:10}}

}


func GetBigLongTrxPrintHeaderLine(headers []string) string {
	//{"binlog", "starttime", "stoptime", "startpos", "stoppos", "rows","duration", "tables"}
	return fmt.Sprintf("%-17s %-19s %-19s %-10s %-10s %-8s %-10s %s\n", ConvertStrArrToIntferfaceArrForPrint(headers)...)
}


func GetStatsPrintHeaderLine(headers []string) string {
	//[binlog, starttime, stoptime, startpos, stoppos, inserts, updates, deletes, database, table,]
	return fmt.Sprintf("%-17s %-19s %-19s %-10s %-10s %-8s %-8s %-8s %-15s %-20s\n", ConvertStrArrToIntferfaceArrForPrint(headers)...)
}


func GetDbTbAndQueryAndRowCntFromBinevent(ev *replication.BinlogEvent) (string, string, string, string, uint32) {
	var (
		db      string = ""
		tb      string = ""
		sql     string = ""
		sqlType string = ""
		rowCnt  uint32 = 0
	)

	switch ev.Header.EventType {

	case replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db = string(wrEvent.Table.Schema)
		tb = string(wrEvent.Table.Table)
		sqlType = "insert"
		rowCnt = uint32(len(wrEvent.Rows))

	case replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db = string(wrEvent.Table.Schema)
		tb = string(wrEvent.Table.Table)
		sqlType = "update"
		rowCnt = uint32(len(wrEvent.Rows)) / 2

	case replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2:

		//replication.XID_EVENT,
		//replication.TABLE_MAP_EVENT:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db = string(wrEvent.Table.Schema)
		tb = string(wrEvent.Table.Table)
		sqlType = "delete"
		rowCnt = uint32(len(wrEvent.Rows))

	case replication.QUERY_EVENT:
		queryEvent := ev.Event.(*replication.QueryEvent)
		db = string(queryEvent.Schema)
		sql = string(queryEvent.Query)
		sqlType = "query"

	case replication.MARIADB_GTID_EVENT:
		// For global transaction ID, used to start a new transaction event group, instead of the old BEGIN query event, and also to mark stand-alone (ddl).
		//https://mariadb.com/kb/en/library/gtid_event/
		sql = "begin"
		sqlType = "query"

	case replication.XID_EVENT:
		// XID_EVENT represents commit。rollback transaction not in binlog
		sql = "commit"
		sqlType = "query"

	}
	return db, tb, sqlType, sql, rowCnt

}




func ProcessBinEventStats(cfg *ConfCmd, wg *sync.WaitGroup) {
	defer wg.Done()

	var (
		lastPrintTime   uint32                         = 0
		lastBinlog      string                         = ""
		statsPrintArr   map[string]*BinEventStatsPrint = map[string]*BinEventStatsPrint{} // key=db.tb
		oneBigLong      BigLongTrxInfo = BigLongTrxInfo{Statements: map[string]map[string]uint32{}}
		//ddlInfoStr      string
		printInterval   uint32 = uint32(cfg.PrintInterval)
		bigTrxRowsLimit uint32 = uint32(cfg.BigTrxRowLimit)
		longTrxSecs     uint32 = uint32(cfg.LongTrxSeconds)
		dbtbKeyes       []string
		//ddlSql          string
	)

	log.Info("start thread to analyze statistics from binlog")
	for st := range cfg.StatChan {

		if lastBinlog != st.Binlog {
			// new binlog
			//print stats
			for _, oneSt := range statsPrintArr {
				cfg.StatFH.WriteString(GetStatsPrintContentLine(oneSt))
			}
			statsPrintArr = map[string]*BinEventStatsPrint{}

			lastPrintTime = 0
		}
		if lastPrintTime == 0 {
			lastPrintTime = st.Timestamp + printInterval
		}
		if lastBinlog == "" {
			lastBinlog = st.Binlog
		}

		dbtbKeyes = []string{}
		if st.QueryType == "query" {
			//fmt.Print(st.QuerySql)
			querySql := strings.ToLower(st.QuerySql)
			//fmt.Printf("query sql:%s\n", querySql)

			// trx cannot spreads in different binlogs
			if querySql == "begin" {
				oneBigLong = BigLongTrxInfo{Binlog: st.Binlog, StartPos: st.StartPos, StartTime: 0, RowCnt: 0, Statements: map[string]map[string]uint32{}}
			} else if querySql == "commit" || querySql == "rollback" {
				if oneBigLong.StartTime > 0 { // the rows event may be skipped by --databases --tables
					//big and long trx
					oneBigLong.StopPos = st.StopPos
					oneBigLong.StopTime = st.Timestamp
					oneBigLong.Duration = oneBigLong.StopTime - oneBigLong.StartTime
					if oneBigLong.RowCnt >= bigTrxRowsLimit || oneBigLong.Duration >= longTrxSecs {
						cfg.BiglongFH.WriteString(GetBigLongTrxContentLine(oneBigLong))
					}
				}

			} 
		} else {
			//big and long trx
			if oneBigLong.Binlog == ""{
				oneBigLong.Binlog = st.Binlog
			}
			if oneBigLong.StartPos == 0{
				oneBigLong.StartPos = st.StartPos
			}

			oneBigLong.RowCnt += st.RowCnt
			dbtbKey := GetAbsTableName(st.Database, st.Table)

			if _, ok := oneBigLong.Statements[dbtbKey]; !ok {
				oneBigLong.Statements[dbtbKey] = map[string]uint32{"insert": 0, "update": 0, "delete": 0}
			}
			oneBigLong.Statements[dbtbKey][st.QueryType] += st.RowCnt
			if oneBigLong.StartTime == 0 {
				oneBigLong.StartTime = st.Timestamp
			}
			dbtbKeyes = append(dbtbKeyes, dbtbKey)

		}
		for _, oneTbKey := range dbtbKeyes {
			//stats
			if _, ok := statsPrintArr[oneTbKey]; !ok {
				statsPrintArr[oneTbKey] = &BinEventStatsPrint{Binlog: st.Binlog, StartTime: st.Timestamp, StartPos: st.StartPos,
					Database: st.Database, Table: st.Table, Inserts: 0, Updates: 0, Deletes: 0}
			}
			switch st.QueryType {
			case "insert":
				statsPrintArr[oneTbKey].Inserts += st.RowCnt
			case "update":
				statsPrintArr[oneTbKey].Updates += st.RowCnt
			case "delete":
				statsPrintArr[oneTbKey].Deletes += st.RowCnt
			}
			statsPrintArr[oneTbKey].StopTime = st.Timestamp
			statsPrintArr[oneTbKey].StopPos = st.StopPos
		}

		if st.Timestamp >= lastPrintTime {

			//print stats
			for _, oneSt := range statsPrintArr {
				cfg.StatFH.WriteString(GetStatsPrintContentLine(oneSt))
			}
			//statFH.WriteString("\n")
			statsPrintArr = map[string]*BinEventStatsPrint{}
			lastPrintTime = st.Timestamp + printInterval

		}

		lastBinlog = st.Binlog

	}
	//print stats
	for _, oneSt := range statsPrintArr {
		cfg.StatFH.WriteString(GetStatsPrintContentLine(oneSt))
	}
	log.Info("exit thread to analyze statistics from binlog")

}

func GetStatsPrintContentLine(st *BinEventStatsPrint) string {
	//[binlog, starttime, stoptime, startpos, stoppos, inserts, updates, deletes, database, table]
	return fmt.Sprintf("%-17s %-19s %-19s %-10d %-10d %-8d %-8d %-8d %-15s %-20s\n",
		st.Binlog, GetDatetimeStr(int64(st.StartTime), int64(0), constvar.DATETIME_FORMAT_NOSPACE),
		GetDatetimeStr(int64(st.StopTime), int64(0), constvar.DATETIME_FORMAT_NOSPACE),
		st.StartPos, st.StopPos, st.Inserts, st.Updates, st.Deletes, st.Database, st.Table)
}

func GetBigLongTrxContentLine(blTrx BigLongTrxInfo) string {
	//{"binlog", "starttime", "stoptime", "startpos", "stoppos", "rows", "duration", "tables"}
	return fmt.Sprintf("%-17s %-19s %-19s %-10d %-10d %-8d %-10d %s\n", blTrx.Binlog,
		GetDatetimeStr(int64(blTrx.StartTime), int64(0), constvar.DATETIME_FORMAT_NOSPACE),
		GetDatetimeStr(int64(blTrx.StopTime), int64(0), constvar.DATETIME_FORMAT_NOSPACE),
		blTrx.StartPos, blTrx.StopPos,
		blTrx.RowCnt, blTrx.Duration, GetBigLongTrxStatementsStr(blTrx.Statements))
}

func GetBigLongTrxStatementsStr(st map[string]map[string]uint32) string {
	strArr := make([]string, len(st))
	var i int = 0
	//var queryTypes []string = []string{"insert", "update", "delete"}
	for dbtb, arr := range st {
		strArr[i] = fmt.Sprintf("%s(inserts=%d, updates=%d, deletes=%d)", dbtb, arr["insert"], arr["update"], arr["delete"])
		i++
	}
	return fmt.Sprintf("[%s]", strings.Join(strArr, " "))
}

