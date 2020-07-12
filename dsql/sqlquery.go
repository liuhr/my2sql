package dsql




type DbTable struct {
	Database string
	Table    string
}

func (this DbTable) Copy() DbTable {
	return DbTable{
		Database: this.Database,
		Table:    this.Table,
	}
}

type SqlInfo struct {
	Tables      []DbTable
	UseDatabase string
	SqlStr      string
	SqlType     int
}

