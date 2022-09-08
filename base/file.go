package base

import (
	"fmt"
	"os"
	"io"
	"bytes"
	"strings"
	"path/filepath"

	"github.com/juju/errors"
	toolkits "my2sql/toolkits"
	"github.com/siddontang/go-log/log"
	"github.com/go-mysql-org/go-mysql/mysql"
        "github.com/go-mysql-org/go-mysql/replication"
)


var (
	fileBinEventHandlingIndex uint64 = 0
	fileTrxIndex              uint64 = 0
)


type BinFileParser struct {
	Parser *replication.BinlogParser
}


func (this BinFileParser) MyParseAllBinlogFiles(cfg *ConfCmd) {
	defer cfg.CloseChan()
	log.Info("start to parse binlog from local files")
	binlog, binpos := GetFirstBinlogPosToParse(cfg)
	binBaseName, binBaseIndx := GetBinlogBasenameAndIndex(binlog)
	log.Info(fmt.Sprintf("start to parse %s %d\n", binlog, binpos))

	for {
		if cfg.IfSetStopFilePos {
			if cfg.StopFilePos.Compare(mysql.Position{Name: filepath.Base(binlog), Pos: 4}) < 1 {
				break
			}
		}

		log.Info(fmt.Sprintf("start to parse %s %d\n", binlog, binpos))
		result, err := this.MyParseOneBinlogFile(cfg, binlog)
		if err != nil {
			log.Error(fmt.Sprintf("error to parse binlog %s %v", binlog, err))
			break
		}

		if result == C_reBreak {
			break
		} else if result == C_reFileEnd {
			if !cfg.IfSetStopParsPoint && !cfg.IfSetStopDateTime {
				//just parse one binlog
				break
			}
			binlog = filepath.Join(cfg.BinlogDir, GetNextBinlog(binBaseName, binBaseIndx))
			if !toolkits.IsFile(binlog) {
				log.Info(fmt.Sprintf("%s not exists nor a file\n", binlog))
				break
			}
			binBaseIndx++
			binpos = 4
		} else {
			log.Info(fmt.Sprintf("this should not happen: return value of MyParseOneBinlog is %d\n", result))
			break
		}

	}
	log.Info("finish parsing binlog from local files")

}

func (this BinFileParser) MyParseOneBinlogFile(cfg *ConfCmd, name string) (int, error) {
	// process: 0, continue: 1, break: 2
	f, err := os.Open(name)
	if f != nil {
		defer f.Close()
	}
	if err != nil {
		log.Error(fmt.Sprintf("fail to open %s %v\n", name, err))
		return C_reBreak, errors.Trace(err)
	}

	fileTypeBytes := int64(4)

	b := make([]byte, fileTypeBytes)
	if _, err = f.Read(b); err != nil {
		log.Error(fmt.Sprintf("fail to read %s %v", name, err))
		return C_reBreak, errors.Trace(err)
	} else if !bytes.Equal(b, replication.BinLogFileHeader) {
		log.Error(fmt.Sprintf("%s is not a valid binlog file, head 4 bytes must fe'bin' ", name))
		return C_reBreak, errors.Trace(err)
	}

	// must not seek to other position, otherwise the program may panic because formatevent, table map event is skipped
	if _, err = f.Seek(fileTypeBytes, os.SEEK_SET); err != nil {
		log.Error(fmt.Sprintf("error seek %s to %d", name, fileTypeBytes))
		return C_reBreak, errors.Trace(err)
	}
	var binlog string = filepath.Base(name)
	return this.MyParseReader(cfg, f, &binlog)
}


func (this BinFileParser) MyParseReader(cfg *ConfCmd, r io.Reader, binlog *string) (int, error) {
	// process: 0, continue: 1, break: 2, EOF: 3
	var (
		err         error
		n           int64
		db          string = ""
		tb          string = ""
		sql         string = ""
		sqlType     string = ""
		rowCnt      uint32 = 0
		trxStatus   int    = 0
		sqlLower    string = ""
		tbMapPos    uint32 = 0
	)

	for {
		headBuf := make([]byte, replication.EventHeaderSize)

		if _, err = io.ReadFull(r, headBuf); err == io.EOF {
			return C_reFileEnd, nil
		} else if err != nil {
			log.Error(fmt.Sprintf("fail to read binlog event header of %s %v", *binlog, err))
			return C_reBreak, errors.Trace(err)
		}


		var h *replication.EventHeader
		h, err = this.Parser.ParseHeader(headBuf)
		if err != nil {
			log.Error(fmt.Sprintf("fail to parse binlog event header of %s %v" , *binlog, err))
			return C_reBreak, errors.Trace(err)
		}
		//fmt.Printf("parsing %s %d %s\n", *binlog, h.LogPos, GetDatetimeStr(int64(h.Timestamp), int64(0), DATETIME_FORMAT))

		if h.EventSize <= uint32(replication.EventHeaderSize) {
			err = errors.Errorf("invalid event header, event size is %d, too small", h.EventSize)
			log.Error("%v", err)
			return C_reBreak, err
		}

		var buf bytes.Buffer
		if n, err = io.CopyN(&buf, r, int64(h.EventSize)-int64(replication.EventHeaderSize)); err != nil {
			err = errors.Errorf("get event body err %v, need %d - %d, but got %d", err, h.EventSize, replication.EventHeaderSize, n)
			log.Error("%v", err)
			return C_reBreak, err
		}


		//h.Dump(os.Stdout)

		data := buf.Bytes()
		var rawData []byte
		rawData = append(rawData, headBuf...)
		rawData = append(rawData, data...)

		eventLen := int(h.EventSize) - replication.EventHeaderSize

		if len(data) != eventLen {
			err = errors.Errorf("invalid data size %d in event %s, less event length %d", len(data), h.EventType, eventLen)
			log.Errorf("%v", err)
			return C_reBreak, err
		}

		var e replication.Event
		e, err = this.Parser.ParseEvent(h, data, rawData)
		if err != nil {
			log.Error(fmt.Sprintf("fail to parse binlog event body of %s %v",*binlog, err))
			return C_reBreak, errors.Trace(err)
		}
		if h.EventType == replication.TABLE_MAP_EVENT {
			tbMapPos = h.LogPos - h.EventSize // avoid mysqlbing mask the row event as unknown table row event
		}

		//e.Dump(os.Stdout)
		//can not advance this check, because we need to parse table map event or table may not found. Also we must seek ahead the read file position
		chRe := CheckBinHeaderCondition(cfg, h, *binlog)
		if chRe == C_reBreak {
			return C_reBreak, nil
		} else if chRe == C_reContinue {
			continue
		} else if chRe == C_reFileEnd {
			return C_reFileEnd, nil
		}

		//binEvent := &replication.BinlogEvent{RawData: rawData, Header: h, Event: e}
		binEvent := &replication.BinlogEvent{Header: h, Event: e} // we donnot need raw data
		oneMyEvent := &MyBinEvent{MyPos: mysql.Position{Name: *binlog, Pos: h.LogPos},
			StartPos: tbMapPos}
		//StartPos: h.LogPos - h.EventSize}
		chRe = oneMyEvent.CheckBinEvent(cfg, binEvent, binlog)
		if chRe == C_reBreak {
			return C_reBreak, nil
		} else if chRe == C_reContinue {
			continue
		} else if chRe == C_reFileEnd {
			return C_reFileEnd, nil
		} 

		db, tb, sqlType, sql, rowCnt = GetDbTbAndQueryAndRowCntFromBinevent(binEvent)
		if sqlType == "query" {
			sqlLower = strings.ToLower(sql)
			if sqlLower == "begin" {
				trxStatus = C_trxBegin
				fileTrxIndex++
			} else if sqlLower == "commit" {
				trxStatus = C_trxCommit
			} else if sqlLower == "rollback" {
				trxStatus = C_trxRollback
			} else if oneMyEvent.QuerySql != nil {
				trxStatus = C_trxProcess
				rowCnt = 1
			}
		} else {
			trxStatus = C_trxProcess
		}


		if cfg.WorkType != "stats" {
			ifSendEvent := false
			if oneMyEvent.IfRowsEvent {

				tbKey := GetAbsTableName(string(oneMyEvent.BinEvent.Table.Schema),
						string(oneMyEvent.BinEvent.Table.Table))
				_, err = G_TablesColumnsInfo.GetTableInfoJson(string(oneMyEvent.BinEvent.Table.Schema),
						string(oneMyEvent.BinEvent.Table.Table))
				if err != nil {
					log.Fatalf(fmt.Sprintf("no table struct found for %s, it maybe dropped, skip it. RowsEvent position:%s",
							tbKey, oneMyEvent.MyPos.String()))
				}
				ifSendEvent = true
			}

			if ifSendEvent {
				fileBinEventHandlingIndex++
				oneMyEvent.EventIdx = fileBinEventHandlingIndex
				oneMyEvent.SqlType = sqlType
				oneMyEvent.Timestamp = h.Timestamp
				oneMyEvent.TrxIndex = fileTrxIndex
				oneMyEvent.TrxStatus = trxStatus
				cfg.EventChan <- *oneMyEvent
			}


		} 

		//output analysis result whatever the WorkType is	
		if sqlType != "" {
			if sqlType == "query" {
				cfg.StatChan <- BinEventStats{Timestamp: h.Timestamp, Binlog: *binlog, StartPos: h.LogPos - h.EventSize, StopPos: h.LogPos,
					Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
			} else {
				cfg.StatChan <- BinEventStats{Timestamp: h.Timestamp, Binlog: *binlog, StartPos: tbMapPos, StopPos: h.LogPos,
					Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
			}
		}


	}

	return C_reFileEnd, nil
}

