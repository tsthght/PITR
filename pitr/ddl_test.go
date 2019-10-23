package pitr

import (
	"fmt"
	"gotest.tools/assert"
	"os"
	"testing"
)

func TestGetAllDatabaseNames(t *testing.T) {
	sql := "use test; create table t1 (a int)"
	sql1 := "create database test1"

	os.RemoveAll(defaultTiDBDir)
	ddl, err := NewDDLHandle()
	assert.Assert(t, err == nil)

	err = ddl.ExecuteDDL(sql)
	assert.Assert(t, err == nil)

	err = ddl.ExecuteDDL(sql1)
	assert.Assert(t, err == nil)

	var n []string
	n, err = ddl.getAllDatabaseNames()
	assert.Assert(t, err == nil)

	assert.Assert(t, len(n) == 2)

	for _, v := range n {
		fmt.Printf("## %s\n", v)
	}
}

func TestResetDB(t *testing.T) {
	sql := "create database test1"
	os.RemoveAll(defaultTiDBDir)
	ddl, err := NewDDLHandle()
	assert.Assert(t, err == nil)

	err = ddl.ResetDB()
	assert.Assert(t, err == nil)

	err = ddl.ExecuteDDL(sql)
	fmt.Printf("## %v\n", err)
	assert.Assert(t, err == nil)

	var ns []string
	ns, err = ddl.getAllDatabaseNames()
	assert.Assert(t, err == nil)
	assert.Assert(t, len(ns) == 2)

	err = ddl.ExecuteDDL(sql)
	assert.Assert(t, err != nil)

	err = ddl.ResetDB()
	assert.Assert(t, err == nil)

	ns, err = ddl.getAllDatabaseNames()
	assert.Assert(t, err == nil)
	assert.Assert(t, len(ns) == 1)

	err = ddl.ExecuteDDL(sql)
	assert.Assert(t, err == nil)

	ns, err = ddl.getAllDatabaseNames()
	assert.Assert(t, err == nil)
	assert.Assert(t, len(ns) == 2)
}
