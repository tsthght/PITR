package pitr

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	tb "github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

const maxMemorySize int64 = 2 * 1024 * 1024 * 1024 // 2G

var (
	defaultTempDir   string = "./temp"
	defaultOutputDir string = "./new_binlog"
)

// Merge used to merge same keys binlog into one
type Merge struct {
	// tempDir used to save splited binlog file
	tempDir string

	// outputDir used to save merged binlog file
	outputDir string

	// which binlog file need merge
	binlogFiles []string

	// memory maybe not enough, need split all binlog files into multiple temp files
	splitNum int

	// schema -> table -> table-file
	//fd map[string]map[string]*os.File

	keyEvent map[string]*Event

	// used for handle ddl, and update table info
	ddlHandle *DDLHandle

	// used for write output binlog
	binlogger binlogfile.Binlogger

	maxCommitTS int64
}

// NewMerge returns a new Merge
func NewMerge(ddls []*model.Job, binlogFiles []string, allFileSize int64) (*Merge, error) {
	if err := os.Mkdir(defaultTempDir, 0700); err != nil {
		return nil, err
	}

	ddlHandle, err := NewDDLHandle(ddls)
	if err != nil {
		return nil, err
	}

	binlogger, err := binlogfile.OpenBinlogger(defaultOutputDir)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Merge{
		tempDir: defaultTempDir,
		//outputDir:   defaultOutputDir,
		binlogFiles: binlogFiles,
		splitNum:    int(allFileSize / maxMemorySize),
		ddlHandle:   ddlHandle,
		keyEvent:    make(map[string]*Event),
		binlogger:   binlogger,
	}, nil
}

// Map split binlog into multiple files
func (m *Merge) Map() error {

	// FIEME: now Map is not implements, so just copy binlog file to temp dir
	for _, bFile := range m.binlogFiles {
		_, fileName := path.Split(bFile)
		_, err := copy(bFile, path.Join(m.tempDir, fileName))
		if err != nil {
			return err
		}
	}

	return nil
}

// Reduce merge same keys binlog into one, and output to file
// every file only contain one table's binlog, just like:
// - output
//   - schema1
//     - table1
//     - table2
//   - schema2
//     - table3
func (m *Merge) Reduce() error {
	fNames, err := binlogfile.ReadDir(m.tempDir)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("reduce", zap.Strings("files", fNames))
	for _, fName := range fNames {
		binlogCh, errCh := m.read(path.Join(m.tempDir, fName))

	Loop:
		for {
			select {
			case binlog, ok := <-binlogCh:
				if ok {
					err := m.analyzeBinlog(binlog)
					if err != nil {
						return err
					}
					m.maxCommitTS = binlog.CommitTs
				} else {
					break Loop
				}
			case err := <-errCh:
				return err
			}
		}
	}

	return m.FlushDMLBinlog(m.maxCommitTS)
}

// FlushDMLBinlog merge some events to one binlog, and then write to file
func (m *Merge) FlushDMLBinlog(commitTS int64) error {
	binlog := m.newDMLBinlog(commitTS)
	i := 0
	for _, row := range m.keyEvent {
		i++
		r := make([][]byte, 0, 10)
		for _, c := range row.cols {
			data, err := c.Marshal()
			if err != nil {
				return err
			}
			r = append(r, data)
		}

		log.Info("generate new event", zap.String("event", fmt.Sprintf("%v", row)))
		newEvent := pb.Event{
			SchemaName: &row.schema,
			TableName:  &row.table,
			Tp:         row.eventType,
			Row:        r,
		}
		binlog.DmlData.Events = append(binlog.DmlData.Events, newEvent)

		// every binlog contain 1000 rows as default
		if i%1000 == 0 {
			err := m.writeBinlog(binlog)
			if err != nil {
				return err
			}
			binlog = m.newDMLBinlog(commitTS)
		}
	}

	if len(binlog.DmlData.Events) != 0 {
		err := m.writeBinlog(binlog)
		if err != nil {
			return err
		}
	}

	// all event have already flush to file, clean these event
	m.keyEvent = make(map[string]*Event)

	return nil
}

func (m *Merge) newDMLBinlog(commitTS int64) *pb.Binlog {
	return &pb.Binlog{
		Tp:       pb.BinlogType_DML,
		CommitTs: commitTS,
		DmlData: &pb.DMLData{
			Events: make([]pb.Event, 0, 1000),
		},
	}
}

func (m *Merge) writeBinlog(binlog *pb.Binlog) error {
	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	_, err = m.binlogger.WriteTail(&tb.Entity{Payload: data})
	return errors.Trace(err)
}

func (m *Merge) Close() {
	if err := os.RemoveAll(m.tempDir); err != nil {
		log.Warn("remove temp dir", zap.String("dir", m.tempDir), zap.Error(err))
	}

	m.ddlHandle.Close()
}

// read reads binlog from pb file
func (m *Merge) read(file string) (chan *pb.Binlog, chan error) {
	binlogChan := make(chan *pb.Binlog, 10)
	errChan := make(chan error)

	go func() {
		f, err := os.OpenFile(file, os.O_RDONLY, 0600)
		if err != nil {
			errChan <- errors.Annotatef(err, "open file %s error", file)
			return
		}

		reader := bufio.NewReader(f)
		for {
			binlog, _, err := Decode(reader)
			if err != nil {
				if errors.Cause(err) == io.EOF {
					log.Info("read file end", zap.String("file", file))
					close(binlogChan)
					return
				} else {
					errChan <- errors.Trace(err)
					return
				}
			}

			binlogChan <- binlog
		}
	}()

	return binlogChan, errChan
}

func (m *Merge) analyzeBinlog(binlog *pb.Binlog) error {
	switch binlog.Tp {
	case pb.BinlogType_DML:
		_, err := m.handleDML(binlog)
		if err != nil {
			return err
		}
	case pb.BinlogType_DDL:
		err := m.ddlHandle.ExecuteDDL(string(binlog.GetDdlQuery()))
		if err != nil {
			return err
		}
		// merge DML events to several binlog and write to file, then write this DDL's binlog
		m.FlushDMLBinlog(binlog.CommitTs - 1)
		m.writeBinlog(binlog)

	default:
		panic("unreachable")
	}
	return nil
}

// handleDML split DML binlog to multiple Event and handle them
func (m *Merge) handleDML(binlog *pb.Binlog) ([]*Event, error) {
	dml := binlog.DmlData
	if dml == nil {
		return nil, errors.New("dml binlog's data can't be empty")
	}

	for _, event := range dml.Events {
		schema := event.GetSchemaName()
		table := event.GetTableName()

		e := &event
		tp := e.GetTp()
		row := e.GetRow()

		var r *Event

		tableInfo, err := m.ddlHandle.GetTableInfo(schema, table)
		if err != nil {
			return nil, err
		}

		switch tp {
		case pb.EventType_Insert, pb.EventType_Delete:
			key, cols, err := getInsertAndDeleteRowKey(row, tableInfo)
			if err != nil {
				return nil, err
			}

			r = &Event{
				schema:    schema,
				table:     table,
				eventType: tp,
				oldKey:    key,
				cols:      cols,
			}

		case pb.EventType_Update:
			key, cKey, cols, err := getUpdateRowKey(row, tableInfo)
			if err != nil {
				return nil, err
			}

			r = &Event{
				schema:    schema,
				table:     table,
				eventType: tp,
				oldKey:    key,
				newKey:    cKey,
				cols:      cols,
			}

		default:
			panic("unreachable")
		}

		m.HandleEvent(r)
	}

	return nil, nil
}

// HandleEvent handles event, if event's key already exist, then merge this event
// otherwise save this event
func (m *Merge) HandleEvent(row *Event) {
	key := row.oldKey
	tp := row.eventType
	oldRow, ok := m.keyEvent[key]
	if ok {
		oldRow.Merge(row)
		if oldRow.isDeleted {
			delete(m.keyEvent, key)
			return
		}

		if tp == pb.EventType_Update {
			// update may change pk/uk value, so key may be changed
			delete(m.keyEvent, key)
			m.keyEvent[oldRow.oldKey] = oldRow
		}
	} else {
		m.keyEvent[row.oldKey] = row
	}
}
