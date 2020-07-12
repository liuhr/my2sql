package base

import (
	"database/sql"
	"fmt"
	"github.com/siddontang/go-log/log"
	toolkits "my2sql/toolkits"
	//"github.com/siddontang/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
)

const (
	//PRIMARY_KEY_LABLE = "primary"
	//UNIQUE_KEY_LABLE  = "unique"
	KEY_BINLOG_POS_SEP = "/"
	KEY_DB_TABLE_SEP   = "."
	KEY_NONE_BINLOG    = "_"

	/*
		KEY_DDL_BINLOG = "binlog"
		KEY_DDL_SPOS   = "startpos"
		KEY_DDL_EPOS   = "stoppos"
	*/
)

var (
	G_TablesColumnsInfo TablesColumnsInfo
)

type DdlPosInfo struct {
	Binlog   string `json:"binlog"`
	StartPos uint32 `json:"start_position"`
	StopPos  uint32 `json:"stop_position"`
	DdlSql   string `json:"ddl_sql"`
}

//{colname1, colname2}
type KeyInfo []string

//type FieldInfo map[string]string //{"name":"col1", "type":"int"}

type FieldInfo struct {
	FieldName string `json:"column_name"`
	FieldType string `json:"column_type"`
}

type TblInfoJson struct {
	Database   string      `json:"database"`
	Table      string      `json:"table"`
	Columns    []FieldInfo `json:"columns"`
	PrimaryKey KeyInfo     `json:"primary_key"`
	UniqueKeys []KeyInfo   `json:"unique_keys"`
	//	DdlInfo    DdlPosInfo  `json:"ddl_info"`
}

type TablesColumnsInfo struct {
	//lock       *sync.RWMutex
	tableInfos map[string]*TblInfoJson //{db.tb:TblInfoJson}}
}

type column struct {
	idx      int
	name     string
	NotNull  bool
	unsigned bool
}

type table struct {
	schema string
	name   string

	columns      []*column
	indexColumns map[string][]*column
}

func GetMysqlUrl(cfg *ConfCmd) string {
	var urlStr string
	urlStr = fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/?autocommit=true&charset=utf8mb4,utf8,latin1&loc=Local&parseTime=true",
			cfg.User, cfg.Passwd, cfg.Host, cfg.Port)
	return urlStr

}

func CreateMysqlCon(mysqlUrl string) (*sql.DB, error) {
	db, err := sql.Open("mysql", mysqlUrl)

	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, err
	}

	err = db.Ping()

	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, err
	}

	return db, nil
}

func (this *TablesColumnsInfo) GetTbDefFromDb(cfg *ConfCmd, dbname string, tbname string) {
	//get table columns from DB
	var err error
	if cfg.FromDB == nil {
		sqlUrl := GetMysqlUrl(cfg)
		cfg.FromDB, err = CreateMysqlCon(sqlUrl)
		if err != nil {
			log.Fatalf("fail to connect to mysql %v", err)
		}
	}

	this.GetTableFieldsFromDb(cfg.FromDB, dbname, tbname)
	this.GetTableKeysInfoFromDb(cfg.FromDB, dbname, tbname)
}

func (this *TablesColumnsInfo) GetTableKeysInfoFromDb(db *sql.DB, dbName string, tbName string) error {
	var (
		kName, colName, ktype string
		colPos                int
		ok                    bool
		dbTbKeysInfo          map[string]map[string]map[string]KeyInfo = map[string]map[string]map[string]KeyInfo{}
		primaryKeys           map[string]map[string]map[string]bool    = map[string]map[string]map[string]bool{}
		sql                   string                                   = `select k.CONSTRAINT_NAME, k.COLUMN_NAME, 
						c.CONSTRAINT_TYPE, k.ORDINAL_POSITION 
						from information_schema.TABLE_CONSTRAINTS as c inner 
						join information_schema.KEY_COLUMN_USAGE as k 
						on c.CONSTRAINT_NAME = k.CONSTRAINT_NAME and c.table_schema = k.table_schema 
						and c.table_name=k.table_name 
						where c.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')
						and c.table_schema ='%s' and c.table_name 
						in ('%s') order by k.table_schema asc, k.table_name asc, k.CONSTRAINT_NAME asc, k.ORDINAL_POSITION asc`
	)
	//log.Infof("geting %s.%s primary/unique keys from mysql",dbName,tbName)
	query := fmt.Sprintf(sql, dbName, tbName)
	rows, err := db.Query(query)
	if err != nil {
		rows.Close()
		log.Errorf("%v fail to query mysql: "+query, err)
		return err
	}
	for rows.Next() {
		err := rows.Scan(&kName, &colName, &ktype, &colPos)
		if err != nil {
			log.Errorf("%v fail to get query result: "+query, err)
			rows.Close()
			return err
		}
		_, ok = dbTbKeysInfo[dbName]
		if !ok {
			dbTbKeysInfo[dbName] = map[string]map[string]KeyInfo{}
		}
		_, ok = dbTbKeysInfo[dbName][tbName]
		if !ok {
			dbTbKeysInfo[dbName][tbName] = map[string]KeyInfo{}
		}
		_, ok = dbTbKeysInfo[dbName][tbName][kName]
		if !ok {
			dbTbKeysInfo[dbName][tbName][kName] = KeyInfo{}
		}
		if !toolkits.ContainsString(dbTbKeysInfo[dbName][tbName][kName], colName) {
			dbTbKeysInfo[dbName][tbName][kName] = append(dbTbKeysInfo[dbName][tbName][kName], colName)
		}

		if ktype == "PRIMARY KEY" {
			_, ok = primaryKeys[dbName]
			if !ok {
				primaryKeys[dbName] = map[string]map[string]bool{}
			}
			_, ok = primaryKeys[dbName][tbName]
			if !ok {
				primaryKeys[dbName][tbName] = map[string]bool{}
			}
			primaryKeys[dbName][tbName][kName] = true
		}
	}
	rows.Close()

	var isPrimay bool = false
	tbKey := GetAbsTableName(dbName, tbName)
	if len(this.tableInfos) < 1 {
		this.tableInfos = map[string]*TblInfoJson{}
	}
	_, ok = this.tableInfos[tbKey]
	if !ok {
		this.tableInfos[tbKey] = &TblInfoJson{
			Database: dbName, Table: tbName,
			PrimaryKey: KeyInfo{}, UniqueKeys: []KeyInfo{}}
	}
	this.tableInfos[tbKey].PrimaryKey = KeyInfo{}
	this.tableInfos[tbKey].UniqueKeys = []KeyInfo{}
	for kn, kf := range dbTbKeysInfo[dbName][tbName] {
		isPrimay = false
		_, ok = primaryKeys[dbName]
		if ok {
			_, ok = primaryKeys[dbName][tbName]
			if ok {
				_, ok = primaryKeys[dbName][tbName][kn]
				if ok && primaryKeys[dbName][tbName][kn] {
					isPrimay = true
				}
			}
		}
		if isPrimay {
			this.tableInfos[tbKey].PrimaryKey = kf
		} else {
			this.tableInfos[tbKey].UniqueKeys = append(this.tableInfos[tbKey].UniqueKeys, kf)
		}
	}
	return nil

}

func (this *TablesColumnsInfo) GetTableFieldsFromDb(db *sql.DB, dbname string, tbname string) error {
	var (
		colName        string
		dataType       string
		colPos         int
		ok             bool
		dbTbFieldsInfo map[string][]FieldInfo = map[string][]FieldInfo{}
		sql            string                 = `select COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION 
						from information_schema.columns 
						where table_schema ='%s' and table_name='%s' 
						order by table_schema asc, table_name asc, ORDINAL_POSITION asc`
	)
	//log.Infof("geting table %s.%s fields from mysql", dbname, tbname)
	query := fmt.Sprintf(sql, dbname, tbname)
	rows, err := db.Query(query)
	if err != nil {
		log.Errorf("%v fail to query mysql: "+query, err)
		rows.Close()
		return err
	}
	tbKey := GetAbsTableName(dbname, tbname)
	for rows.Next() {
		err := rows.Scan(&colName, &dataType, &colPos)
		if err != nil {
			log.Infof("%v error to get query result: "+query, err)
			rows.Close()
			return err
		}

		_, ok = dbTbFieldsInfo[tbKey]
		if !ok {
			dbTbFieldsInfo[tbKey] = []FieldInfo{}
		}

		dbTbFieldsInfo[tbKey] = append(dbTbFieldsInfo[tbKey], FieldInfo{FieldName: colName, FieldType: dataType})
	}
	rows.Close()
	if len(this.tableInfos) < 1 {
		this.tableInfos = map[string]*TblInfoJson{}
	}
	this.tableInfos[tbKey] = &TblInfoJson{Database: dbname, Table: tbname, Columns: dbTbFieldsInfo[tbKey]}
	return nil
}

func (this *TablesColumnsInfo) GetTableInfoJson(schema string, table string) (*TblInfoJson, error) {
	tbKey := GetAbsTableName(schema, table)
	tbDefsJson, ok := this.tableInfos[tbKey]
	if !ok {
		this.GetTbDefFromDb(GConfCmd, schema, table)
		tbDefsJson, ok = this.tableInfos[tbKey]
		if !ok {
			return &TblInfoJson{}, fmt.Errorf("table struct not found for %s, maybe it was dropped. Skip it", tbKey)
		}
	}
	return tbDefsJson, nil
}

func (this *TblInfoJson) GetOneUniqueKey(uniqueFirst bool) KeyInfo {
	if uniqueFirst {
		if len(this.UniqueKeys) > 0 {
			return this.UniqueKeys[0]
		}
	}
	if len(this.PrimaryKey) > 0 {
		return this.PrimaryKey
	} else if len(this.UniqueKeys) > 0 {
		return this.UniqueKeys[0]
	} else {
		return KeyInfo{}
	}
}

func GetColIndexFromKey(ki KeyInfo, columns []FieldInfo) []int {
	arr := make([]int, len(ki))
	for j, colName := range ki {
		for i, f := range columns {
			if f.FieldName == colName {
				arr[j] = i
				break
			}
		}
	}
	return arr
}
