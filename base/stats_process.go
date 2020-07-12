package base

import (
	"fmt"
	//"github.com/openark/golib/log"
	"github.com/siddontang/go-log/log"
	"my2sql/dsql"
	"os"
	"path/filepath"
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

func OpenStatsResultFiles(cfg *ConfCmd) (*os.File, *os.File, *os.File) {
	// stat file
	statFile := filepath.Join(cfg.OutputDir, "binlog_status.txt")
	statFH, err := os.OpenFile(statFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("fail to open file %v"+statFile, err)
	}

	statFH.WriteString(GetStatsPrintHeaderLine(Stats_Result_Header_Column_names))

	// ddl file
	ddlFile := filepath.Join(cfg.OutputDir, "ddl_info.txt")
	ddlFH, err := os.OpenFile(ddlFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		statFH.Close()
		log.Fatalf("fail to open file %v"+ddlFile, err)
	}

	ddlFH.WriteString(GetDdlPrintHeaderLine(Stats_DDL_Header_Column_names))

	// biglong trx info
	biglongFile := filepath.Join(cfg.OutputDir, "biglong_trx.txt")
	biglongFH, err := os.OpenFile(biglongFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		statFH.Close()
		ddlFH.Close()
		log.Fatalf("fail to open file %v"+biglongFile, err)

	}

	biglongFH.WriteString(GetBigLongTrxPrintHeaderLine(Stats_BigLongTrx_Header_Column_names))

	return statFH, ddlFH, biglongFH
	//return bufio.NewWriter(statFH), bufio.NewWriter(ddlFH), bufio.NewWriter(biglongFH)
}

func GetBigLongTrxPrintHeaderLine(headers []string) string {
	//{"binlog", "starttime", "stoptime", "startpos", "stoppos", "rows","duration", "tables"}
	return fmt.Sprintf("%-17s %-19s %-19s %-10s %-10s %-8s %-10s %s\n", ConvertStrArrToIntferfaceArrForPrint(headers)...)
}

func GetDdlPrintHeaderLine(headers []string) string {
	//{"datetime", "binlog", "startpos", "stoppos", "sql"}
	return fmt.Sprintf("%-19s %-17s %-10s %-10s %s\n", ConvertStrArrToIntferfaceArrForPrint(headers)...)
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
	//fmt.Println(db, tb, sqlType, rowCnt, sql)
	return db, tb, sqlType, sql, rowCnt

}
