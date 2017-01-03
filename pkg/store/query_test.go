package store

import (
	"bufio"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/prototype/pkg/fs"
)

func TestQueryEngineNaïve(t *testing.T)   { testQueryEngine(t, QueryEngineNaïve) }
func TestQueryEngineRipgrep(t *testing.T) { testQueryEngine(t, QueryEngineRipgrep) }
func TestQueryEngineLazy(t *testing.T)    { testQueryEngine(t, QueryEngineLazy) }

func testQueryEngine(t *testing.T, engine QueryEngine) {
	storeRoots := os.Getenv("STORE_ROOTS")
	if storeRoots == "" {
		t.Skip("skipping; set STORE_ROOTS to run")
	}

	var logs []Log
	for _, root := range strings.Fields(storeRoots) {
		log, err := NewFileLog(fs.NewRealFilesystem(), root, 1000000)
		if err != nil {
			t.Fatalf("%s: %v", root, err)
		}
		logs = append(logs, log)
	}

	var (
		from, _   = time.Parse(time.RFC3339, "2017-01-03T00:36:01+01:00")
		to, _     = time.Parse(time.RFC3339, "2017-01-04T00:38:59+01:00")
		q         = "7"
		statsOnly = false
	)

	scatterQuery(t, logs, engine, from, to, q, statsOnly)
}

func scatterQuery(t *testing.T, logs []Log, engine QueryEngine, from, to time.Time, q string, statsOnly bool) {
	var (
		overallBegin = time.Now()
		resultc      = make(chan QueryResult, len(logs))
		errc         = make(chan error, len(logs))
	)
	for _, log := range logs {
		go func(log Log) {
			if res, err := log.Query(engine, from, to, q, statsOnly); err != nil {
				errc <- err
			} else {
				resultc <- res
			}
		}(log)
	}

	var results []QueryResult
	for range logs {
		select {
		case res := <-resultc:
			results = append(results, res)
		case err := <-errc:
			t.Fatalf("Got error: %v", err)
		}
	}

	t.Logf("Query phase complete in %s", time.Since(overallBegin))

	var wg sync.WaitGroup
	wg.Add(len(results))
	for _, res := range results {
		if res.Records == nil {
			t.Logf("nil Records?")
			continue
		}
		go func(r io.Reader) {
			defer wg.Done()
			var (
				readBegin = time.Now()
				count     = 0
				bytes     = 0
			)
			s := bufio.NewScanner(r)
			for s.Scan() {
				count++
				bytes += len(s.Bytes())
			}
			took := time.Since(readBegin)
			t.Logf(
				"Read %d record(s), %d byte(s) in %s -- %.2f MBps",
				count, bytes, took,
				float64(bytes/1024/1024)/took.Seconds(),
			)
		}(res.Records)
	}
	wg.Wait()

	t.Logf("Took %s", time.Since(overallBegin))
}
