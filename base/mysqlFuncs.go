package base

import (
	"database/sql"
	"fmt"
	"strings"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	toolkits "my2sql/toolkits"
	_ "github.com/go-sql-driver/mysql"
)

const (
	//PRIMARY_KEY_LABLE = "primary"
	//UNIQUE_KEY_LABLE  = "unique"
	KEY_BINLOG_POS_SEP = "/"
	KEY_DB_TABLE_SEP   = "."
	KEY_NONE_BINLOG    = "_"

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

	this.GetTableColumns(cfg.FromDB, dbname, tbname)
	this.GetTableKeysInfo(cfg.FromDB, dbname, tbname)
}

func (this *TablesColumnsInfo) GetTableKeysInfo(db *sql.DB, dbName string, tbName string) error {
	var (
		ok                    bool
		dbTbKeysInfo          map[string]map[string]map[string]KeyInfo = map[string]map[string]map[string]KeyInfo{}
		primaryKeys           map[string]map[string]map[string]bool    = map[string]map[string]map[string]bool{}
	)

	if dbName == "" || tbName == "" {
		er := "schema/table is empty"
		log.Errorf(er)
		return errors.New(er)
	}

	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", dbName, tbName)
	rows, err := db.Query(query)
	if err != nil {
		log.Errorf("%v fail to query mysql: "+query, err)
		return err
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		log.Errorf("get columns name err %v",err)
		return errors.Trace(err)
	}

	// Show an example.
	/*
		mysql> show index from test.t;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| t     |          0 | PRIMARY  |            1 | a           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | PRIMARY  |            2 | b           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | ucd      |            1 | c           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		| t     |          0 | ucd      |            2 | d           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/
	
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))
		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			log.Errorf("rows scan err %v",err)
			return errors.Trace(err)
		}

		nonUnique := string(data[1])
		if nonUnique == "0" {
			//if strings.ToLower(string(data[2])) == "PRIMARY" {
			//}
			_, ok = dbTbKeysInfo[dbName]
			if !ok {
				dbTbKeysInfo[dbName] = map[string]map[string]KeyInfo{}
			}
			_, ok = dbTbKeysInfo[dbName][tbName]
			if !ok {
				dbTbKeysInfo[dbName][tbName] = map[string]KeyInfo{}
			}
			kName := string(data[2])
			_, ok = dbTbKeysInfo[dbName][tbName][kName]
			if !ok {
				dbTbKeysInfo[dbName][tbName][kName] = KeyInfo{}
			}
			colName := string(data[4])
			if !toolkits.ContainsString(dbTbKeysInfo[dbName][tbName][kName], colName) {
				dbTbKeysInfo[dbName][tbName][kName] = append(dbTbKeysInfo[dbName][tbName][kName], colName)
			}

			if strings.Contains(strings.ToLower(kName), "primary") {
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
	}

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
	for kname, kcolumn := range dbTbKeysInfo[dbName][tbName] {
		isPrimay = false
		_, ok = primaryKeys[dbName]
		if ok {
			_, ok = primaryKeys[dbName][tbName]
			if ok {
				_, ok = primaryKeys[dbName][tbName][kname]
				if ok && primaryKeys[dbName][tbName][kname] {
					isPrimay = true
				}
			}
		}
		if isPrimay {
			this.tableInfos[tbKey].PrimaryKey = kcolumn
		} else {
			this.tableInfos[tbKey].UniqueKeys = append(this.tableInfos[tbKey].UniqueKeys, kcolumn)
		}
	}
	return nil
}


func  (this *TablesColumnsInfo) GetTableColumns(db *sql.DB, dbname string, tbname string) error{
	var (
		dbTbFieldsInfo map[string][]FieldInfo = map[string][]FieldInfo{}
	)

	if dbname == "" || tbname == "" {
		er := "schema/table is empty"
		log.Errorf(er)
		return errors.New(er)
	}

	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", dbname, tbname)
	rows, err := db.Query(query)
	if err != nil {
		log.Errorf("%v fail to query mysql: "+query, err)
		return err
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		log.Errorf("get rows columns err %v",err)
		return errors.Trace(err)
	}

	// Show an example.
	/*
	   mysql> show columns from test.tb;
	   +-------+---------+------+-----+---------+-------+
	   | Field | Type    | Null | Key | Default | Extra |
	   +-------+---------+------+-----+---------+-------+
	   | a     | int(11) | NO   | PRI | NULL    |       |
	   | b     | int(11) | NO   | PRI | NULL    |       |
	   | c     | int(11) | YES  | MUL | NULL    |       |
	   | d     | int(11) | YES  |     | NULL    |       |
	   +-------+---------+------+-----+---------+-------+
	*/

	tbKey := GetAbsTableName(dbname, tbname)
	for rows.Next() {
		//err := rows.Scan(&colName, &dataType, &nullValue, &KeyValue, &defaultValue, &extraValue)
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))
		for i := range values {
			values[i] = &data[i]
		}
		err = rows.Scan(values...)
		if err != nil {
			log.Errorf("rows scan err %v",err)
			return errors.Trace(err)
		}
		_, ok := dbTbFieldsInfo[tbKey]
		if !ok {
			dbTbFieldsInfo[tbKey] = []FieldInfo{}
		}
		dbTbFieldsInfo[tbKey] = append(dbTbFieldsInfo[tbKey], FieldInfo{FieldName: string(data[0]), FieldType: GetFiledType(string(data[1]))})
	}
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
