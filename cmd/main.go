// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/tsthght/PITR/pitr"
	"go.uber.org/zap"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := pitr.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatal("verifying flags failed. See 'pitr --help'.", zap.Error(err))
	}

	if err := util.InitLogger(cfg.LogLevel, cfg.LogFile); err != nil {
		log.Fatal("Failed to initialize log", zap.Error(err))
	}
	version.PrintVersionInfo("PITR")

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	r, err := pitr.New(cfg)
	if err != nil {
		log.Fatal("create pitr failed", zap.Error(err))
	}

	go func() {
		sig := <-sc
		log.Info("got signal to exit.", zap.Stringer("signale", sig))
		r.Close()
		os.Exit(0)
	}()

	if err := r.Process(); err != nil {
		log.Error("pitr processing failed", zap.Error(err))
	}
	if err := r.Close(); err != nil {
		log.Fatal("close pitr failed", zap.Error(err))
	}
}
