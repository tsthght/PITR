package pitr

import (
	"os"
	"testing"

	tb "github.com/pingcap/tipb/go-binlog"
	"gotest.tools/assert"
)

func TestMapFunc1(t *testing.T) {
	dstPath := "./test_map"
	srcPath := "./maptest"
	os.RemoveAll(dstPath + "/")
	os.RemoveAll(srcPath + "/")
	os.RemoveAll(defaultTiDBDir)
	os.RemoveAll(defaultTempDir)

	//generate files

	b, err := OpenMyBinlogger(srcPath)
	assert.Assert(t, err == nil)

	bin := genTestDDL("test", "tb1", "use test;create table tb1 (a int primary key, b int, c int)", 100)
	data, _ := bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	bin = genTestDML("test", "tb1", 200)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	bin = genTestDDL("test", "tb1", "use test; drop table tb1", 201)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	bin = genTestDDL("test", "tb1", "use test; create table tb1 (a int primary key, b int, c int)", 202)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	bin = genTestDDL("test", "tb2", "use test; create table tb2 (a int primary key, b int, c int)", 203)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})
	bin = genTestDML("test", "tb2", 204)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	bin = genTestDML("test", "tb2", 205)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	b.Close()

	files, err := searchFiles(srcPath)
	assert.Assert(t, err == nil)

	files, fileSize, err := filterFiles(files, 0, 300)
	assert.Assert(t, err == nil)

	merge, err := NewMerge(files, fileSize)
	assert.Assert(t, err == nil)

	err = merge.Map()
	assert.Assert(t, err == nil)

	tb1, err := searchFiles(merge.tempDir + "/" + "test_tb1")
	assert.Assert(t, err == nil)
	tb1f, _, err := filterFiles(tb1, 0, 300)
	assert.Assert(t, err == nil)
	assert.Assert(t, len(tb1f) == 3)

	tb2, err := searchFiles(merge.tempDir + "/" + "test_tb2")
	assert.Assert(t, err == nil)
	tb2f, _, err := filterFiles(tb2, 0, 300)
	assert.Assert(t, err == nil)
	assert.Assert(t, len(tb2f) == 2)

	merge.Close()
	merge.ddlHandle.Close()
	os.RemoveAll(dstPath + "/")
	os.RemoveAll(srcPath + "/")
}
