package base

import (
	"bufio"
	"fmt"
	"my2sql/sqltypes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	SQL "my2sql/sqlbuilder"
	constvar "my2sql/constvar"
	"github.com/siddontang/go-log/log"

)

type ExtraSqlInfoOfPrint struct {
	schema    string
	table     string
	binlog    string
	startpos  uint32
	endpos    uint32
	datetime  string
	trxIndex  uint64
	trxStatus int
}

type ForwardRollbackSqlOfPrint struct {
	sqls    []string
	sqlInfo ExtraSqlInfoOfPrint
}

var (
	ForwardSqlFileNamePrefix  string = "forward"
	RollbackSqlFileNamePrefix string = "rollback"
)

func GenForwardRollbackSqlFromBinEvent(i uint, cfg *ConfCmd, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		err error
		//var currentIdx uint64
		tbInfo             *TblInfoJson
		db, tb, fulltb     string
		allColNames        []FieldInfo
		colsDef            []SQL.NonAliasColumn
		colsTypeName       []string
		colCnt             int
		sqlArr             []string
		uniqueKeyIdx       []int
		uniqueKey          KeyInfo
		primaryKeyIdx      []int
		ifRollback         bool = false
		ifIgnorePrimary    bool = cfg.IgnorePrimaryKeyForInsert
		currentSqlForPrint ForwardRollbackSqlOfPrint
		posStr             string
		//printStatementSql  bool = false
	)
	log.Infof(fmt.Sprintf("start thread %d to generate redo/rollback sql", i))
	if cfg.WorkType == "rollback" {
		ifRollback = true
	}

	for ev := range cfg.EventChan {
		if !ev.IfRowsEvent {
			continue
		}
		posStr = GetPosStr(ev.MyPos.Name, ev.StartPos, ev.MyPos.Pos)
		db = string(ev.BinEvent.Table.Schema)
		tb = string(ev.BinEvent.Table.Table)
		fulltb = GetAbsTableName(db, tb)
		tbInfo, err = G_TablesColumnsInfo.GetTableInfoJson(db, tb)
		if err != nil {
			log.Errorf(fmt.Sprintf("error to found %s table structure for event", fulltb))
			continue
		}
		if tbInfo == nil {
			log.Errorf("no suitable table struct found for %s for event %s", fulltb, posStr)
		}
		colCnt = len(ev.BinEvent.Rows[0])
		allColNames = GetAllFieldNamesWithDroppedFields(colCnt, tbInfo.Columns)
		colsDef, colsTypeName = GetSqlFieldsEXpressions(colCnt, allColNames, ev.BinEvent.Table)
		colsTypeNameFromMysql := make([]string, len(colsTypeName))
		if len(colsTypeName) > len(tbInfo.Columns) {
			log.Fatalf("%s column count %d in binlog > in table structure %d, usually means DDL in the middle", fulltb, len(colsTypeName), len(tbInfo.Columns))
		}
		for ci, colType := range colsTypeName {
			colsTypeNameFromMysql[ci] = tbInfo.Columns[ci].FieldType

			if strings.Contains(strings.ToLower(colType), "int") {
				if tbInfo.Columns[ci].IsUnsigned {
					for ri, _ := range ev.BinEvent.Rows {
						ev.BinEvent.Rows[ri][ci] = sqltypes.ConvertIntUnsigned(ev.BinEvent.Rows[ri][ci], colType)
					}

				}
			}
			
			if colType == "blob" {
				// text is stored as blob
				if strings.Contains(strings.ToLower(tbInfo.Columns[ci].FieldType), "text") {
					for ri, _ := range ev.BinEvent.Rows {
						if ev.BinEvent.Rows[ri][ci] == nil {
							continue
						}
						txtStr, coOk := ev.BinEvent.Rows[ri][ci].([]byte)
						if !coOk {
							log.Fatalf("%s.%s %v []byte  empty %s", fulltb, allColNames[ci].FieldName, ev.BinEvent.Rows[ri][ci], posStr)
						} else {
							ev.BinEvent.Rows[ri][ci] = string(txtStr)
						}
					}
				}
			}
			/*if colType == "json" {
				for ri, _ := range ev.BinEvent.Rows {
					if ev.BinEvent.Rows[ri][ci] == nil {
						continue
					}
					txtStr, coOk := ev.BinEvent.Rows[ri][ci].([]byte)
					if !coOk {
						log.Fatalf("%s.%s %v []byte  empty %s", fulltb, allColNames[ci].FieldName, ev.BinEvent.Rows[ri][ci], posStr)
					} else {
						ev.BinEvent.Rows[ri][ci] = string(txtStr)
					}
				}

			}*/
		}
		uniqueKey = tbInfo.GetOneUniqueKey(cfg.UseUniqueKeyFirst)
		if len(uniqueKey) > 0 {
			uniqueKeyIdx = GetColIndexFromKey(uniqueKey, allColNames)
		} else {
			uniqueKeyIdx = []int{}
		}

		if len(tbInfo.PrimaryKey) > 0 {
			primaryKeyIdx = GetColIndexFromKey(tbInfo.PrimaryKey, allColNames)
		} else {
			primaryKeyIdx = []int{}
			ifIgnorePrimary = false
		}

		if ev.SqlType == "insert" {
			if ifRollback {
				sqlArr = GenDeleteSqlsForOneRowsEventRollbackInsert(posStr, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.FullColumns, cfg.SqlTblPrefixDb)
			} else {
				sqlArr = GenInsertSqlsForOneRowsEvent(posStr, ev.BinEvent, colsDef, 1, false, cfg.SqlTblPrefixDb, ifIgnorePrimary, primaryKeyIdx)
			}
		} else if ev.SqlType == "delete" {
			if ifRollback {
				sqlArr = GenInsertSqlsForOneRowsEventRollbackDelete(posStr, ev.BinEvent, colsDef, 1, cfg.SqlTblPrefixDb)
			} else {
				sqlArr = GenDeleteSqlsForOneRowsEvent(posStr, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.FullColumns, false, cfg.SqlTblPrefixDb)
			}
		} else if ev.SqlType == "update" {
			if ifRollback {
				sqlArr = GenUpdateSqlsForOneRowsEvent(posStr, colsTypeNameFromMysql, colsTypeName, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.FullColumns, true, cfg.SqlTblPrefixDb)
			} else {
				sqlArr = GenUpdateSqlsForOneRowsEvent(posStr, colsTypeNameFromMysql, colsTypeName, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.FullColumns, false, cfg.SqlTblPrefixDb)
			}
		} else {
			fmt.Println("unsupported query type %s to generate 2sql|rollback sql, it should one of insert|update|delete. %s", ev.SqlType, ev.MyPos.String())
			continue
		}
		currentSqlForPrint = ForwardRollbackSqlOfPrint{sqls: sqlArr,
			sqlInfo: ExtraSqlInfoOfPrint{schema: db, table: tb, binlog: ev.MyPos.Name, startpos: ev.StartPos, endpos: ev.MyPos.Pos,
				datetime: GetDatetimeStr(int64(ev.Timestamp), int64(0), constvar.DATETIME_FORMAT_NOSPACE),
				trxIndex: ev.TrxIndex, trxStatus: ev.TrxStatus}}

		for {
			//fmt.Println("in thread", i)
			G_HandlingBinEventIndex.lock.Lock()
			//fmt.Println("handing index:", G_HandlingBinEventIndex.EventIdx, "binevent index:", ev.EventIdx)
			if G_HandlingBinEventIndex.EventIdx == ev.EventIdx {
				if cfg.OutputToScreen {
					for _, sql := range currentSqlForPrint.sqls {
						fmt.Println(sql)
					}
				} else {
					cfg.SqlChan <- currentSqlForPrint
				}
				G_HandlingBinEventIndex.EventIdx++
				G_HandlingBinEventIndex.lock.Unlock()
				//fmt.Println("handing index == binevent index, break")
				break
			}

			G_HandlingBinEventIndex.lock.Unlock()
			time.Sleep(1 * time.Microsecond)

		}
	}
	log.Infof(fmt.Sprintf("exit thread %d to generate redo/rollback sql", i))
}

func PrintExtraInfoForForwardRollbackupSql(cfg *ConfCmd, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		rollbackFileName string                   = ""
		tmpFileName      string                   = ""
		oneSqls          string                   = ""
		fhArr            map[string]*os.File      = map[string]*os.File{}
		fhArrBuf         map[string]*bufio.Writer = map[string]*bufio.Writer{}
		FH               *os.File
		bufFH            *bufio.Writer
		err              error
		rollbackFiles    []map[string]string //{"tmp":xx, "rollback":xx}
		//lastTrxIndex     uint64 = 0
		//trxStr           string = "commit;\nbegin;\n"
		// trxStrLen int = len(trxStr)
		//trxCommitStr string = "commit;\n"
		// trxCommitStrLen int = len(trxCommitStr)
		bytesCntFiles      map[string][][]int = map[string][][]int{} //{"file1":{{8, 0}, {8 , 0}}} {length of bytes, trxIndex}
		lastPrintPos       uint32             = 0
		lastPrintFile      string             = ""
		printBytesInterval uint32             = 1024 * 1024 * 10 //every 10MB print process info
	)
	log.Infof(fmt.Sprintf("start thread to write redo/rollback sql into file"))
	for sc := range cfg.SqlChan {
		if cfg.WorkType == "rollback" {
			tmpFileName = GetForwardRollbackSqlFileName(sc.sqlInfo.schema, sc.sqlInfo.table, cfg.FilePerTable, cfg.OutputDir, true, sc.sqlInfo.binlog, true)
			rollbackFileName = GetForwardRollbackSqlFileName(sc.sqlInfo.schema, sc.sqlInfo.table, cfg.FilePerTable, cfg.OutputDir, true, sc.sqlInfo.binlog, false)
		} else {
			tmpFileName = GetForwardRollbackSqlFileName(sc.sqlInfo.schema, sc.sqlInfo.table, cfg.FilePerTable, cfg.OutputDir, false, sc.sqlInfo.binlog, false)
		}
		if _, ok := fhArr[tmpFileName]; !ok {
			FH, err = os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {

			}
			bufFH = bufio.NewWriter(FH)
			fhArrBuf[tmpFileName] = bufFH
			fhArr[tmpFileName] = FH
			if cfg.WorkType == "rollback" {
				rollbackFiles = append(rollbackFiles, map[string]string{"tmp": tmpFileName, "rollback": rollbackFileName})
				bytesCntFiles[tmpFileName] = [][]int{}
			}
		}

		//lastTrxIndex = sc.sqlInfo.trxIndex
		oneSqls = GetForwardRollbackContentLineWithExtra(sc, cfg.PrintExtraInfo)
		fhArrBuf[tmpFileName].WriteString(oneSqls)
		if lastPrintFile == "" {
			lastPrintFile = sc.sqlInfo.binlog
		}
		if sc.sqlInfo.binlog != lastPrintFile {
			lastPrintPos = 0
			lastPrintFile = sc.sqlInfo.binlog
			log.Infof(fmt.Sprintf("finish processing %s %d", sc.sqlInfo.binlog, sc.sqlInfo.endpos))
		} else if sc.sqlInfo.endpos-lastPrintPos >= printBytesInterval {
			lastPrintPos = sc.sqlInfo.endpos
			log.Infof(fmt.Sprintf("finish processing %s %d", sc.sqlInfo.binlog, sc.sqlInfo.endpos))
		}

		if cfg.WorkType == "rollback" {
			bytesCntFiles[tmpFileName] = append(bytesCntFiles[tmpFileName], []int{len(oneSqls), int(sc.sqlInfo.trxIndex)})
		}
	}

	for fn, bufFH := range fhArrBuf {
		bufFH.Flush()
		fhArr[fn].Close()
	}

	// reverse rollback sql file
	if cfg.WorkType == "rollback" {
		log.Info("finish writing rollback sql into tmp files, start to revert content order of tmp files")
		var reWg sync.WaitGroup
		filesChan := make(chan map[string]string, cfg.Threads)
		threadNum := GetMinValue(int(cfg.Threads), len(rollbackFiles))
		for i := 1; i <= threadNum; i++ {
			reWg.Add(1)
			go ReverseFileGo(i, filesChan, bytesCntFiles, cfg.KeepTrx, &reWg)
		}
		for _, tmpArr := range rollbackFiles {
			filesChan <- tmpArr
		}
		close(filesChan)
		reWg.Wait()
		log.Info("finish reverting content order of tmp files")
	} else {
		log.Info("finish writing redo/forward sql into file")
	}

	log.Info("exit thread to write redo/rollback sql into file")
}

func GetForwardRollbackSqlFileName(schema string, table string, filePerTable bool, outDir string, ifRollback bool, binlog string, ifTmp bool) string {

	_, idx := GetBinlogBasenameAndIndex(binlog)

	if ifRollback {
		if ifTmp {
			if filePerTable {
				return filepath.Join(outDir, fmt.Sprintf(".%s.%s.%s.%d.sql", schema, table, RollbackSqlFileNamePrefix, idx))
			} else {
				return filepath.Join(outDir, fmt.Sprintf(".%s.%d.sql", RollbackSqlFileNamePrefix, idx))
			}

		} else {
			if filePerTable {
				return filepath.Join(outDir, fmt.Sprintf("%s.%s.%s.%d.sql", schema, table, RollbackSqlFileNamePrefix, idx))
			} else {
				return filepath.Join(outDir, fmt.Sprintf("%s.%d.sql", RollbackSqlFileNamePrefix, idx))
			}
		}
	} else {
		if filePerTable {
			return filepath.Join(outDir, fmt.Sprintf("%s.%s.%s.%d.sql", schema, table, ForwardSqlFileNamePrefix, idx))
		} else {
			return filepath.Join(outDir, fmt.Sprintf("%s.%d.sql", ForwardSqlFileNamePrefix, idx))
		}

	}

}

func GetForwardRollbackContentLineWithExtra(sq ForwardRollbackSqlOfPrint, ifExtra bool) string {
	if ifExtra {
		return fmt.Sprintf("# datetime=%s database=%s table=%s binlog=%s startpos=%d stoppos=%d\n%s;\n",
			sq.sqlInfo.datetime, sq.sqlInfo.schema, sq.sqlInfo.table, sq.sqlInfo.binlog, sq.sqlInfo.startpos,
			sq.sqlInfo.endpos, strings.Join(sq.sqls, ";\n"))
	} else {

		str := strings.Join(sq.sqls, ";\n") + ";\n"
		return str
	}

}
