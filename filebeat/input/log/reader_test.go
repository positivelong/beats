// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build !integration
// +build !integration

package log

import (
	"fmt"
	"github.com/elastic/beats/filebeat/harvester"
	"github.com/elastic/beats/filebeat/input/file"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/reader"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

var lines = []string{
	"This is line 1",
	"This is line 2",
	"This is line 3",
}

type closeTrackingSource struct {
	harvester.Source
	mu      sync.Mutex
	closed  bool
	closeCh chan struct{}
}

func (s *closeTrackingSource) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	close(s.closeCh)
	return s.Source.Close()
}

func (s *closeTrackingSource) wasClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

type blockingReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingReader() *blockingReader {
	return &blockingReader{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (r *blockingReader) Next() (reader.Message, error) {
	r.once.Do(func() { close(r.started) })
	<-r.release
	return reader.Message{}, ErrClosed
}

func TestReloadFileOffsetStopsReaderBeforeClosingFD(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "reload.log")
	content := "first line\n"
	assert.NoError(t, os.WriteFile(logFile, []byte(content), 0o600))

	harvester, err := getHarvester(logFile, int64(len(content)))
	assert.NoError(t, err)
	reuseReader := &ReuseHarvester{
		HarvesterID: harvester.id,
		Config:      harvester.config,
		State:       harvester.state,
	}
	fileHarvester, err := newFileHarvester(reuseReader)
	assert.NoError(t, err)
	t.Cleanup(func() {
		fileHarvester.stopReader()
		fileHarvester.readerDone.Wait()
		fileHarvester.closeFile()
		fileHarvester.Close()
	})

	oldSource := &closeTrackingSource{
		Source:  fileHarvester.source,
		closeCh: make(chan struct{}),
	}
	fileHarvester.source = oldSource
	fileHarvester.log.fs = oldSource
	fileHarvester.forwarders[harvester.id] = &ReuseHarvester{
		HarvesterID: harvester.id,
		State:       file.State{Offset: 0},
	}

	oldLog := fileHarvester.log
	blockedReader := newBlockingReader()
	fileHarvester.reader = blockedReader
	assert.True(t, fileHarvester.startReader())

	select {
	case <-blockedReader.started:
	case <-time.After(time.Second):
		t.Fatal("reader did not start")
	}

	reloadDone := make(chan error, 1)
	go func() {
		_, _, reloadErr := fileHarvester.reloadFileOffset()
		reloadDone <- reloadErr
	}()

	select {
	case <-oldLog.done:
	case <-oldSource.closeCh:
		t.Fatal("old source was closed before the read loop was stopped")
	case <-time.After(time.Second):
		t.Fatal("reader stop was not requested")
	}
	assert.False(t, oldSource.wasClosed())

	close(blockedReader.release)
	select {
	case err = <-reloadDone:
	case <-time.After(time.Second):
		t.Fatal("reload did not finish after the old reader exited")
	}
	assert.NoError(t, err)
	assert.True(t, oldSource.wasClosed())
}

func TestReloadFileOffsetDoesNotStartDuplicateReader(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "reload-noop.log")
	assert.NoError(t, os.WriteFile(logFile, []byte("first line\n"), 0o600))

	harvester, err := getHarvester(logFile, 0)
	assert.NoError(t, err)
	reuseReader := &ReuseHarvester{
		HarvesterID: harvester.id,
		Config:      harvester.config,
		State:       harvester.state,
	}
	fileHarvester, err := newFileHarvester(reuseReader)
	assert.NoError(t, err)
	t.Cleanup(func() {
		fileHarvester.stopReader()
		fileHarvester.readerDone.Wait()
		fileHarvester.closeFile()
		fileHarvester.Close()
	})

	blockedReader := newBlockingReader()
	fileHarvester.reader = blockedReader
	fileHarvester.forwarders[harvester.id] = reuseReader
	assert.True(t, fileHarvester.startReader())

	select {
	case <-blockedReader.started:
	case <-time.After(time.Second):
		t.Fatal("reader did not start")
	}

	offset, reopened, err := fileHarvester.reloadFileOffset()
	assert.NoError(t, err)
	assert.Equal(t, fileHarvester.state.Offset, offset)
	assert.False(t, reopened)
	assert.False(t, fileHarvester.startReader(), "an active reader must not be started twice")

	close(blockedReader.release)
	fileHarvester.readerDone.Wait()
}

func TestReuseReadLine(t *testing.T) {
	absPath, err := filepath.Abs("../../tests/files/logs/")
	logFile := absPath + "/tmp" + strconv.Itoa(rand.Int()) + ".log"
	err = genLogFile(logFile, lines)
	if err != nil {
		t.Fatalf("Error creating the absolute path: %s", absPath)
	}
	defer func() {
		os.Remove(logFile)
	}()

	wg := &sync.WaitGroup{}

	harvesterNums := 100
	wg.Add(harvesterNums)

	for i := 0; i < harvesterNums; i++ {
		go startHarvester(t, i, wg, logFile, lines)
	}

	wg.Wait()
}

func TestReuseCleanup(t *testing.T) {
	absPath, err := filepath.Abs("../../tests/files/logs/")
	logFile := absPath + "/tmp" + strconv.Itoa(rand.Int()) + ".log"
	err = genLogFile(logFile, lines)
	if err != nil {
		t.Fatalf("Error creating the absolute path: %s", absPath)
	}
	h1, err := getHarvester(logFile, 0)
	if err != nil {
		t.Logf("harvester get reader err: %v", err)
		return
	}
	fileReader, err := NewReuseHarvester(h1.id, h1.config, h1.state)
	if err != nil {
		panic(err)
	}
	os.Remove(logFile)
	for i := 0; i <= len(lines); i++ {
		fileReader.Next()
	}
	fileReaderManager.cleanup()
}

func genLogFile(logFile string, lines []string) error {
	_, err := os.Stat(logFile)
	if err == nil {
		os.Remove(logFile)
	}

	fd, err := os.Create(logFile)
	if err != nil {
		return err
	}
	defer func() {
		fd.Close()
	}()

	for _, line := range lines {
		_, err = fd.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}
	err = fd.Sync()
	if err != nil {
		return err
	}
	_, err = os.Stat(logFile)
	if err != nil {
		return err
	}
	return nil
}

func startHarvester(
	t *testing.T,
	id int,
	wg *sync.WaitGroup,
	logFile string,
	lines []string,
) {
	var fileReader *ReuseHarvester
	defer func() {
		t.Logf("harvester-%d is stopped", id)
		wg.Done()
	}()
	h1, err := getHarvester(logFile, 0)
	if err != nil {
		t.Logf("harvester-%d get reader err: %v", id, err)
		return
	}
	t.Logf("harvester-%d is trying to get the reader", id)

	time.Sleep(time.Duration(rand.Intn(100)) * time.Microsecond)

	fileReader, err = NewReuseHarvester(h1.id, h1.config, h1.state)
	if err != nil {
		panic(err)
	}
	t.Logf("harvester-%d has get the reader", id)

	//read lines
	for i, line := range lines {
		message, _ := fileReader.Next()
		assert.Equal(
			t,
			fmt.Sprintf("[%d]%s", id, line),
			fmt.Sprintf("[%d]%s", id, string(message.Content)))
		t.Logf("harvester-%d has get %d line", id, i+1)
	}
	t.Logf("harvester-%d trying to stop", id)
	fileReader.Stop()
}

func getHarvester(filePath string, offset int64) (*Harvester, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	vars := map[string]interface{}{
		"type":         "log",
		"paths":        []string{filePath},
		"encoding":     "utf-8",
		"reuse_reader": true,
	}
	rawConfig, err := common.NewConfigFrom(vars)
	if err != nil {
		return nil, err
	}

	h := &Harvester{
		id:     id,
		config: defaultConfig,
		states: file.NewStates(),
	}

	if err := rawConfig.Unpack(&h.config); err != nil {
		panic(err)
	}

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	fileState := file.NewState(fileInfo, filePath, "log", nil, file.IdentifierInode)
	fileState.Offset = offset
	h.state = fileState
	return h, nil
}
