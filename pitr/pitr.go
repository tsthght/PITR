package pitr

import (
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"go.uber.org/zap"
)

// PITR is the main part of the merge binlog tool.
type PITR struct {
	cfg    *Config

	filter *filter.Filter
}

// New creates a PITR object.
func New(cfg *Config) (*PITR, error) {
	log.Info("New PITR", zap.Stringer("config", cfg))

	filter := filter.NewFilter(cfg.IgnoreDBs, cfg.IgnoreTables, cfg.DoDBs, cfg.DoTables)

	return &PITR{
		cfg:    cfg,
		filter: filter,
	}, nil
}

// Process runs the main procedure.
func (r *PITR) Process() error {
	pbReader, err := newDirPbReader(r.cfg.Dir, r.cfg.StartTSO, r.cfg.StopTSO)
	if err != nil {
		return errors.Annotatef(err, "new reader failed dir: %s", r.cfg.Dir)
	}
	defer pbReader.close()

	for {
		binlog, err := pbReader.read()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}

			return errors.Trace(err)
		}

		log.Info("process", zap.Reflect("binlog", binlog))
		/*
		ignore, err := filterBinlog(r.filter, binlog)
		if err != nil {
			return errors.Annotate(err, "filter binlog failed")
		}

		if ignore {
			continue
		}
		*/
	}
}

// Close closes the PITR object.
func (r *PITR) Close() error {
	return nil
}

/*
// may drop some DML event of binlog
// return true if the whole binlog should be ignored
func filterBinlog(afilter *filter.Filter, binlog *pb.Binlog) (ignore bool, err error) {
	switch binlog.Tp {
	case pb.BinlogType_DDL:
		var table filter.TableName
		_, table, err = parseDDL(string(binlog.GetDdlQuery()))
		if err != nil {
			return false, errors.Annotatef(err, "parse ddl: %s failed", string(binlog.GetDdlQuery()))
		}

		if afilter.SkipSchemaAndTable(table.Schema, table.Table) {
			return true, nil
		}

		return
	case pb.BinlogType_DML:
		var events []pb.Event
		for _, event := range binlog.DmlData.GetEvents() {
			if afilter.SkipSchemaAndTable(event.GetSchemaName(), event.GetTableName()) {
				continue
			}

			events = append(events, event)
		}

		binlog.DmlData.Events = events
		if len(events) == 0 {
			ignore = true
		}
		return
	default:
		return false, errors.Errorf("unknown type: %d", binlog.Tp)
	}
}
*/

func isAcceptableBinlog(binlog *pb.Binlog, startTs, endTs int64) bool {
	return binlog.CommitTs >= startTs && (endTs == 0 || binlog.CommitTs <= endTs)
}
