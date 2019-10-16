package pltr

import (
	"database/sql"

	tidblite "github.com/WangXiangUSTC/tidb-lite"
)

const (
	colsSQL = `
SELECT column_name, extra FROM information_schema.columns
WHERE table_schema = ? AND table_name = ?;`
	uniqKeysSQL = `
SELECT non_unique, index_name, seq_in_index, column_name 
FROM information_schema.statistics
WHERE table_schema = ? AND table_name = ?
ORDER BY seq_in_index ASC;`
)

type DDLHandle struct {
	db *sql.DB

	tableInfos sync.Map
}

func NewDDLHandle() (*DDLHandle, error) {
	tidbServer, err := tidblite.NewTiDBServer(tidblite.NewOptions(c.MkDir()).WithPort(4040))
	if err != nil {
		return nil, err
	}

	var dbConn *sql.DB
	for i := 0; i < 5; i++ {
		dbConn, err = tidbServer.CreateConn()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}

	if err != nil {
		return nil, err
	}

	return &DDLHandle{
		db: dbConn,
	}, nil
}

func (d *DDLHandle) ExecuteDDL(ddl string) error {
	if err := d.db.Execute(ddl); err != nil {
		return errors.Trace(err)
	}

	info, err = getTableInfo(d.db, schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	s.tableInfos.Store(quoteSchema(schema, table), info)
}

func (d *DDLHandle) GetTableInfo(schema, table string) (*tableInfo, error) {
	v, ok := d.tableInfos.Load(quoteSchema(schema, table))
	if ok {
		info = v.(*tableInfo)
		return
	}
	return getTableInfo(d.db, schema, table)
}

type tableInfo struct {
	columns    []string
	primaryKey *indexInfo
	// include primary key if have
	uniqueKeys []indexInfo
}

type indexInfo struct {
	name    string
	columns []string
}

/ getTableInfo returns information like (non-generated) column names and
// unique keys about the specified table
func getTableInfo(db *gosql.DB, schema string, table string) (info *tableInfo, err error) {
	info = new(tableInfo)

	if info.columns, err = getColsOfTbl(db, schema, table); err != nil {
		if err == ErrTableNotExist {
			return nil, err
		}
		return nil, errors.Trace(err)
	}

	if info.uniqueKeys, err = getUniqKeys(db, schema, table); err != nil {
		return nil, errors.Trace(err)
	}

	// put primary key at first place
	// and set primaryKey
	for i := 0; i < len(info.uniqueKeys); i++ {
		if info.uniqueKeys[i].name == "PRIMARY" {
			info.uniqueKeys[i], info.uniqueKeys[0] = info.uniqueKeys[0], info.uniqueKeys[i]
			info.primaryKey = &info.uniqueKeys[0]
			break
		}
	}

	return
}

// getColsOfTbl returns a slice of the names of all columns,
// generated columns are excluded.
// https://dev.mysql.com/doc/mysql-infoschema-excerpt/5.7/en/columns-table.html
func getColsOfTbl(db *sql.DB, schema, table string) ([]string, error) {
	rows, err := db.Query(colsSQL, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	cols := make([]string, 0, 1)
	for rows.Next() {
		var name, extra string
		err = rows.Scan(&name, &extra)
		if err != nil {
			return nil, errors.Trace(err)
		}
		isGenerated := strings.Contains(extra, "VIRTUAL GENERATED") || strings.Contains(extra, "STORED GENERATED")
		if isGenerated {
			continue
		}
		cols = append(cols, name)
	}

	if err = rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	// if no any columns returns, means the table not exist.
	if len(cols) == 0 {
		return nil, ErrTableNotExist
	}

	return cols, nil
}