package pitr

import (
	"fmt"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
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

	ddls, err := r.loadHistoryDDLJobs()
	if err != nil {
		return errors.Annotate(err, "load history ddls")
	}

	merge, err := NewMerge(ddls, files, fileSize)
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

	return nil
}

// Close closes the PITR object.
func (r *PITR) Close() error {
	return nil
}

func isAcceptableBinlog(binlog *pb.Binlog, startTs, endTs int64) bool {
	return binlog.CommitTs >= startTs && (endTs == 0 || binlog.CommitTs <= endTs)
}

func (r *PITR) loadHistoryDDLJobs() ([]*model.Job, error) {
	tiStore, err := createTiStore(r.cfg.PDURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer tiStore.Close()

	snapMeta, err := getSnapshotMeta(tiStore)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// jobs from GetAllHistoryDDLJobs are sorted by job id, need sorted by schema version
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].BinlogInfo.SchemaVersion < jobs[j].BinlogInfo.SchemaVersion
	})

	return jobs, nil
}

func createTiStore(urls string) (kv.Storage, error) {
	urlv, err := flags.NewURLsValue(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := store.Register("tikv", tikv.Driver{}); err != nil {
		return nil, errors.Trace(err)
	}
	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := store.New(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return tiStore, nil
}

func getSnapshotMeta(tiStore kv.Storage) (*meta.Meta, error) {
	version, err := tiStore.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot, err := tiStore.GetSnapshot(version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta.NewSnapshotMeta(snapshot), nil
}
