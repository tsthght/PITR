package pitr

import (
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

type Row struct {
	eventType  pb.EventType

	oldKey string
	newKey string

	oldValue []byte
	newValue []byte
}

func (r *Row) Merge(newRow *Row) {
	/*
	if r.key != newRow.key {
		// can't merge
		return
	}
	*/

	if r.eventType == pb.EventType_Insert {
		if  newRow.eventType == pb.EventType_Insert {
			// this should never happened
		} else if newRow.eventType == pb.EventType_Delete {
			// this row is deleted
			r = nil
		} else if newRow.eventType == pb.EventType_Update {
			// update the newValue
			r.eventType = pb.EventType_Insert
			r.oldValue = newRow.newValue

		}
	} else if r.eventType == pb.EventType_Update {
		if  newRow.eventType == pb.EventType_Insert {
			// this should not be happen
		} else if newRow.eventType == pb.EventType_Delete {
			// this row is deleted
			r = newRow
		} else if newRow.eventType == pb.EventType_Update {
			// update the newValue
			r.newValue = newRow.newValue
		}
	} else if r.eventType == pb.EventType_Delete {
		if  newRow.eventType == pb.EventType_Insert {
			r = newRow
		} else if newRow.eventType == pb.EventType_Delete {
			// this should never happened
		} else if newRow.eventType == pb.EventType_Update {
			// this should never happened
		}
	}
}

func analyzeBinlog(binlog *pb.Binlog) []*Row {
	switch binlog.Tp {
	case pb.BinlogType_DML:
		sqls, args, err = translateDML(binlog)
		return sqls, args, false, errors.Trace(err)
	case pb.BinlogType_DDL:
		sqls, args, err = translateDDL(binlog)
		return sqls, args, true, errors.Trace(err)
	default:
		panic("unreachable")
	}
}

func translateDML(binlog *pb.Binlog) ([]*Row, error) {
	dml := binlog.DmlData
	if dml == nil {
		return nil, nil, errors.New("dml binlog's data can't be empty")
	}

	sqls := make([]string, 0, len(dml.Events))
	args := make([][]interface{}, 0, len(dml.Events))

	var (
		sql string
		arg []interface{}
		err error
	)

	for _, event := range dml.Events {

		e := &event
		tp := e.GetTp()
		row := e.GetRow()

		switch tp {
		case pb.EventType_Insert:
			sql, arg, err = r.translator.TransInsert(binlog, e, row)
		case pb.EventType_Update:
			sql, arg, err = r.translator.TransUpdate(binlog, e, row)
		case pb.EventType_Delete:
			sql, arg, err = r.translator.TransDelete(binlog, e, row)
		default:
			panic("unreachable")
		}
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		sqls = append(sqls, sql)
		args = append(args, arg)
	}

	return sqls, args, nil
}