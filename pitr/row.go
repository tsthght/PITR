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
