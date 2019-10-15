package pitr

const maxMemorySize int64 = 2 * 1024 * 1024 * 1024 // 2G

// Merge used to merge same keys binlog into one
type Merge struct {
	// tempDir used to save splited binlog file
	tempDir string

	// which binlog file need merge
	binlogFiles []string

	// memory maybe not enough, need split all binlog files into multiple temp files
	splitNum int
}

// NewMerge returns a new Merge
func NewMerge(binlogFiles []string, allFileSize int64) *Merge {
	return &Merge{
		tempDir:     "./temp",
		binlogFiles: binlogFiles,
		splitNum:    int(allFileSize / maxMemorySize),
	}
}

// Map split binlog into multiple files
func (m *Merge) Map() {

}

// Reduce merge same keys binlog into one, and output to file
// every file only contain one table's binlog, just like:
// - output
//   - schema1
//     - table1
//     - table2
//   - schema2
//     - table3
func (m *Merge) Reduce() {

}
