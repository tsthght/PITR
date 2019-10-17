package pitr

import (
	//"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"go.uber.org/zap"
)

// PITR is the main part of the merge binlog tool.
type PITR struct {
	cfg *Config

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
	files, err := searchFiles(r.cfg.Dir)
	if err != nil {
		return errors.Annotate(err, "searchFiles failed")
	}

	files, fileSize, err := filterFiles(files, r.cfg.StartTSO, r.cfg.StopTSO)
	if err != nil {
		return errors.Annotate(err, "filterFiles failed")
	}
	merge, err := NewMerge(files, fileSize)
	if err != nil {
		return errors.Trace(err)
	}

	defer merge.Close()
	
	if err := merge.Map(); err != nil {
		return errors.Trace(err)
	}
	
	if err := merge.Reduce(); err != nil {
		return errors.Trace(err)
	}
	

	/*
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
	}
	*/

	return nil
}

// Close closes the PITR object.
func (r *PITR) Close() error {
	return nil
}

func isAcceptableBinlog(binlog *pb.Binlog, startTs, endTs int64) bool {
	return binlog.CommitTs >= startTs && (endTs == 0 || binlog.CommitTs <= endTs)
}
