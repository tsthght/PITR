package pitr

import (
	"os"
	"path"
	"bufio"
	"io"
	"fmt"

	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"go.uber.org/zap"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

const maxMemorySize int64 = 2 * 1024 * 1024 * 1024 // 2G

var (
	defaultTempDir string = "./temp"
)

// Merge used to merge same keys binlog into one
type Merge struct {
	// tempDir used to save splited binlog file
	tempDir string

	// which binlog file need merge
	binlogFiles []string

	// memory maybe not enough, need split all binlog files into multiple temp files
	splitNum int

	// schema -> table -> table-file
	fd map[string]map[string]*os.File
}

// NewMerge returns a new Merge
func NewMerge(binlogFiles []string, allFileSize int64) (*Merge, error) {
	if err := os.Mkdir(defaultTempDir, 0700); err != nil {
		return nil, err
	}
	
	return &Merge{
		tempDir:     defaultTempDir,
		binlogFiles: binlogFiles,
		splitNum:    int(allFileSize / maxMemorySize),
	}, nil
}

// Map split binlog into multiple files
func (m *Merge) Map() error {
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
			case binlog := <-binlogCh:
				log.Info("read binlog", zap.Reflect("binlog", binlog))
			case err := <-errCh:
				if errors.Cause(err) == io.EOF {
					log.Info("read file end", zap.String("file", fName))
					break Loop
				}
				return err
			}
		}
	}

	return nil
}

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
				errChan <- errors.Trace(err)
				return
			}

			binlogChan <- binlog
		}
	}()

	return binlogChan, errChan
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