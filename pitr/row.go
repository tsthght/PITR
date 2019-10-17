package pitr

import (
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/log"
	//"go.uber.org/zap"
)

type Row struct {
	schema string
	table string

	eventType  pb.EventType

	oldKey string
	newKey string

	//oldValue []byte
	//newValue []byte

	cols []*pb.Column

	isDeleted bool
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
			log.Info("insert to insert")
		} else if newRow.eventType == pb.EventType_Delete {
			// this row is deleted
			log.Info("insert to delete")
			r.isDeleted = true
		} else if newRow.eventType == pb.EventType_Update {
			// update the newValue
			log.Info("insert to update")
			r.eventType = pb.EventType_Insert
			//r.oldValue = newRow.newValue
			r.oldToNew(newRow)
			r.oldKey = newRow.newKey
		}
	} else if r.eventType == pb.EventType_Update {
		if  newRow.eventType == pb.EventType_Insert {
			log.Info("update to insert")
			// this should not be happen
		} else if newRow.eventType == pb.EventType_Delete {
			// this row is deleted
			log.Info("update to delete")
			r = newRow
		} else if newRow.eventType == pb.EventType_Update {
			// update the newValue
			log.Info("update to update")
			//r.newValue = newRow.newValue
			r.newToNew(newRow)
		}
	} else if r.eventType == pb.EventType_Delete {
		if  newRow.eventType == pb.EventType_Insert {
			log.Info("delete to insert")
			r = newRow
		} else if newRow.eventType == pb.EventType_Delete {
			log.Info("delete to delete")
			// this should never happened
		} else if newRow.eventType == pb.EventType_Update {
			log.Info("delete to update")
			// this should never happened
		}
	}
}

func (r *Row) oldToNew(newRow *Row) {
	for i, col := range newRow.cols {
		r.cols[i].Value = col.ChangedValue
	}
}

func (r *Row) newToNew(newRow *Row) {
	for i, col := range newRow.cols {
		r.cols[i].ChangedValue = col.ChangedValue
	}
}