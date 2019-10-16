package pitr

import (
	"os"
	"fmt"
	"io"
	"strings"

	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"go.uber.org/zap"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/parser/mysql"

)

// event.GetRow()
func getRowKey(row [][]byte, info *tableInfo) (string, error) {
	//cols = make(map[])
	values := make(map[string]interface{})

	//cols = make([]string, 0, len(row))
	//args = make([]interface{}, 0, len(row))
	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return "", errors.Trace(err)
		}
		//cols = append(cols, col.Name)

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return "", errors.Trace(err)
		}

		tp := col.Tp[0]
		val = formatValue(val, tp)
		log.Debug("format value",
			zap.String("col name", col.Name),
			zap.String("mysql type", col.MysqlType),
			zap.Reflect("value", val.GetValue()))
		//args = append(args, val.GetValue())
		values[col.Name] = val.GetValue()
	}
	var key string
	var columns []string
	if info.primaryKey != nil {
		columns = info.primaryKey.columns
	} else {
		columns = info.columns
	}
	for _, col := range columns {
		key += fmt.Sprintf("%v|", values[col])
	}

	return key, nil
}

func copy(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
			return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
			return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
			return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
			return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func quoteSchema(schema string, table string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(table))
}

func quoteName(name string) string {
	return "`" + escapeName(name) + "`"
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

// parserSchemaTableFromDDL parses ddl query to get schema and table
// ddl like `use test; create table`
func parserSchemaTableFromDDL(ddlQuery string) (schema, table string, err error) {
	stmts, _, err := parser.New().Parse(ddlQuery, "", "")
	if err != nil {
		return "", "", err
	}

	haveUseStmt := false

	for _, stmt := range stmts {
		switch node := stmt.(type) {
		case *ast.UseStmt:
			haveUseStmt = true
			schema = node.DBName
		case *ast.CreateDatabaseStmt:
			schema = node.Name
		case *ast.DropDatabaseStmt:
			schema = node.Name
		case *ast.TruncateTableStmt:
			if len(node.Table.Schema.O) != 0 {
				schema = node.Table.Schema.O
			}
			table = node.Table.Name.O
		case *ast.CreateIndexStmt:
			if len(node.Table.Schema.O) != 0 {
				schema = node.Table.Schema.O
			}
			table = node.Table.Name.O
		case *ast.CreateTableStmt:
			if len(node.Table.Schema.O) != 0 {
				schema = node.Table.Schema.O
			}
			table = node.Table.Name.O
		case *ast.DropIndexStmt:
			if len(node.Table.Schema.O) != 0 {
				schema = node.Table.Schema.O
			}
			table = node.Table.Name.O
		case *ast.AlterTableStmt:
			if len(node.Table.Schema.O) != 0 {
				schema = node.Table.Schema.O
			}
			table = node.Table.Name.O
		case *ast.DropTableStmt:
			// FIXME: may drop more than one table in a ddl
			if len(node.Tables[0].Schema.O) != 0 {
				schema = node.Tables[0].Schema.O
			}
			table = node.Tables[0].Name.O
		case *ast.RenameTableStmt:
			if len(node.NewTable.Schema.O) != 0 {
				schema = node.NewTable.Schema.O
			}
			table = node.NewTable.Name.O
		default:
			return "", "", errors.Errorf("unknown ddl type, ddl: %s", ddlQuery)
		}
	}

	if haveUseStmt {
		if len(stmts) != 2 {
			return "", "", errors.Errorf("invalid ddl %s", ddlQuery)
		}
	} else {
		if len(stmts) != 1 {
			return "", "", errors.Errorf("invalid ddl %s", ddlQuery)
		}
	}

	return
}

func formatValue(value types.Datum, tp byte) types.Datum {
	if value.GetValue() == nil {
		return value
	}

	switch tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeDecimal, mysql.TypeNewDecimal, mysql.TypeVarchar, mysql.TypeString, mysql.TypeJSON:
		value = types.NewDatum(fmt.Sprintf("%s", value.GetValue()))
	case mysql.TypeEnum:
		value = types.NewDatum(value.GetMysqlEnum().Value)
	case mysql.TypeSet:
		value = types.NewDatum(value.GetMysqlSet().Value)
	case mysql.TypeBit:
		value = types.NewDatum(value.GetMysqlBit())
	}

	return value
}