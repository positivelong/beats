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
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func TestReloadFileOffsetRestartsSingleReaderWhenOffsetUnchanged(t *testing.T) {
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

	oldLog := fileHarvester.log
	reloadDone := make(chan struct{})
	var offset int64
	var reopened bool
	go func() {
		offset, reopened, err = fileHarvester.reloadFileOffset()
		close(reloadDone)
	}()

	select {
	case <-oldLog.done:
	case <-time.After(time.Second):
		t.Fatal("active reader was not stopped before reload")
	}
	close(blockedReader.release)
	select {
	case <-reloadDone:
	case <-time.After(time.Second):
		t.Fatal("reload did not finish after the old reader exited")
	}
	assert.NoError(t, err)
	assert.Equal(t, fileHarvester.state.Offset, offset)
	assert.True(t, reopened)
	assert.True(t, fileHarvester.startReader(), "reloaded reader must start exactly once")
	assert.False(t, fileHarvester.startReader(), "reloaded reader must not be started twice")
}

func TestReloadFileOffsetPreservesForwarderOffsets(t *testing.T) {
	for _, ludicrousMode := range []bool{false, true} {
		for _, firstFromStart := range []bool{false, true} {
			t.Run(fmt.Sprintf("ludicrous_mode=%t/first-from-start=%t", ludicrousMode, firstFromStart), func(t *testing.T) {
				testReplayEncodingOffsets(t, ludicrousMode, "utf-8", nil, 0, firstFromStart, false)
			})
		}
	}
}

func TestReloadFileOffsetPreservesBOMOffsets(t *testing.T) {
	for _, tc := range []struct {
		name     string
		encoding string
		order    unicode.Endianness
		bom      unicode.BOMPolicy
	}{
		{"le", "utf-16le-bom", unicode.LittleEndian, unicode.UseBOM},
		{"be", "utf-16be-bom", unicode.BigEndian, unicode.UseBOM},
		{"auto-le", "utf-16-bom", unicode.LittleEndian, unicode.UseBOM},
		{"auto-be", "utf-16-bom", unicode.BigEndian, unicode.UseBOM},
		{"le-no-bom", "utf-16le-bom", unicode.LittleEndian, unicode.IgnoreBOM},
		{"be-no-bom", "utf-16be-bom", unicode.BigEndian, unicode.IgnoreBOM},
	} {
		for _, batch := range []bool{false, true} {
			for _, fromStart := range []bool{false, true} {
				for _, resumed := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/batch=%t/first-from-start=%t/resumed=%t", tc.name, batch, fromStart, resumed), func(t *testing.T) {
						bomBytes := int64(0)
						if tc.bom == unicode.UseBOM {
							bomBytes = 2
						}
						testReplayEncodingOffsets(t, batch, tc.encoding, unicode.UTF16(tc.order, tc.bom).NewEncoder(), bomBytes, fromStart, resumed)
					})
				}
			}
		}
	}
}

func testReplayEncodingOffsets(t *testing.T, ludicrousMode bool, encodingName string, encoder transform.Transformer, bomBytes int64, firstFromStart, resumed bool) {
	logFile := filepath.Join(t.TempDir(), "reload-batch.log")
	allLines := make([]string, 10)
	var content strings.Builder
	for i := range allLines {
		allLines[i] = fmt.Sprintf("msg-%06d", i)
		content.WriteString(allLines[i])
		content.WriteByte('\n')
	}
	raw := []byte(content.String())
	if encoder != nil {
		var err error
		raw, _, err = transform.Bytes(encoder, raw)
		require.NoError(t, err)
	}
	require.NoError(t, os.WriteFile(logFile, raw, 0o600))

	lineBytes := int64(len(allLines[0]) + 1)
	if encoder != nil {
		lineBytes *= 2
	}
	aOffset := bomBytes + 3*lineBytes
	harvester, err := getHarvester(logFile, aOffset)
	require.NoError(t, err)
	harvester.config.LudicrousMode = ludicrousMode
	harvester.config.BufferSize = 8 * int(lineBytes)
	harvester.config.Encoding = encodingName
	harvester.config.CloseEOF = true // A regression must fail, not wait forever at EOF.

	forwarderA := newBufferedReuseHarvester(harvester, aOffset)
	bOffset := int64(0)
	bStartLine := 0
	if resumed {
		bOffset = bomBytes + lineBytes
		bStartLine = 1
	}
	forwarderB := newBufferedReuseHarvester(harvester, bOffset)
	first := forwarderA
	if firstFromStart {
		first = forwarderB
	}
	fileHarvester, err := newFileHarvester(first)
	require.NoError(t, err)
	forwarderA.fileReader = fileHarvester
	forwarderB.fileReader = fileHarvester
	fileHarvester.forwarders[forwarderA.HarvesterID] = forwarderA
	fileHarvester.forwarders[forwarderB.HarvesterID] = forwarderB
	t.Cleanup(func() {
		fileHarvester.stopReader()
		fileHarvester.readerDone.Wait()
		fileHarvester.closeFile()
		fileHarvester.Close()
	})

	offset, reopened, err := fileHarvester.reloadFileOffset()
	require.NoError(t, err)
	require.True(t, reopened, "different task offsets require replay boundaries")
	expectedOffset := bOffset
	if expectedOffset == 0 {
		expectedOffset = bomBytes
	}
	require.Equal(t, expectedOffset, offset)
	require.Equal(t, offset, fileHarvester.state.Offset)
	require.Equal(t, expectedOffset, forwarderB.State.Offset)
	require.Equal(t, aOffset, forwarderA.State.Offset)
	fileHarvester.state.Offset = offset

	fileSize := int64(len(raw))
	for fileHarvester.state.Offset < fileSize {
		message, readErr := fileHarvester.reader.Next()
		if !assert.NoError(t, readErr) {
			return
		}
		if message.Bytes <= 0 {
			t.Fatalf("reader returned a non-positive byte count: %d", message.Bytes)
		}
		fileHarvester.forward(message, nil)
		fileHarvester.state.Offset += int64(message.Bytes)
	}

	assert.Equal(t, allLines[3:], bufferedLines(forwarderA.message))
	assert.Equal(t, allLines[bStartLine:], bufferedLines(forwarderB.message))
	assert.Equal(t, fileSize, forwarderA.State.Offset)
	assert.Equal(t, fileSize, forwarderB.State.Offset)
}

func newBufferedReuseHarvester(harvester *Harvester, offset int64) *ReuseHarvester {
	state := harvester.state
	state.Offset = offset
	return &ReuseHarvester{
		HarvesterID: uuid.Must(uuid.NewV4()),
		Config:      harvester.config,
		State:       state,
		done:        make(chan struct{}),
		message:     make(chan ReuseMessage, 16),
	}
}

// Exercise the real Run/forwarder channel/loopRead path as well as direct reload.
func TestRunPreservesBOMOffsets(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, lateJoin := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%t/late-join=%t", batch, lateJoin), func(t *testing.T) {
				logFile := filepath.Join(t.TempDir(), "bom-run.log")
				allLines := []string{"msg-000001", "msg-000002", "msg-000003", "msg-000004", "msg-000005", "msg-000006"}
				raw, _, err := transform.Bytes(unicode.UTF16(unicode.LittleEndian, unicode.UseBOM).NewEncoder(), []byte(strings.Join(allLines, "\n")+"\n"))
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(logFile, raw, 0o600))
				aOffset := int64(2 + 3*22)
				h, err := getHarvester(logFile, aOffset)
				require.NoError(t, err)
				h.config.Encoding = "utf-16le-bom"
				h.config.LudicrousMode = batch
				a := newBufferedReuseHarvester(h, aOffset)
				b := newBufferedReuseHarvester(h, 0)
				fh, err := newFileHarvester(a)
				require.NoError(t, err)
				a.fileReader, b.fileReader = fh, fh
				finished := make(chan struct{})
				go func() { defer close(finished); fh.Run() }()
				t.Cleanup(func() {
					fh.Close()
					select {
					case <-finished:
					case <-time.After(5 * time.Second):
						t.Error("Run did not shut down")
					}
				})
				join := func(r *ReuseHarvester) {
					select {
					case fh.forwarder <- r:
					case <-time.After(5 * time.Second):
						t.Fatal("forwarder did not join")
					}
				}
				collect := func(r *ReuseHarvester, count int) []string {
					var result []string
					timer := time.NewTimer(10 * time.Second)
					defer timer.Stop()
					for len(result) < count {
						select {
						case msg := <-r.message:
							require.NoError(t, msg.error)
							result = append(result, strings.Split(string(msg.message.Content), "\n")...)
						case <-timer.C:
							t.Fatalf("received %d of %d lines", len(result), count)
						}
					}
					return result
				}
				join(a)
				var aLines []string
				if lateJoin {
					aLines = collect(a, 3)
				}
				join(b)
				bLines := collect(b, 6)
				if !lateJoin {
					aLines = collect(a, 3)
				}
				fh.Close()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Fatal("Run did not finish")
				}
				assert.Equal(t, allLines[3:], append(aLines, bufferedLines(a.message)...))
				assert.Equal(t, allLines, append(bLines, bufferedLines(b.message)...))
				assert.Equal(t, int64(len(raw)), fh.state.Offset)
				assert.Equal(t, int64(len(raw)), a.State.Offset)
				assert.Equal(t, int64(len(raw)), b.State.Offset)
			})
		}
	}
}

func bufferedLines(messages chan ReuseMessage) []string {
	lines := make([]string, 0)
	for len(messages) > 0 {
		message := <-messages
		lines = append(lines, strings.Split(string(message.message.Content), "\n")...)
	}
	return lines
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
