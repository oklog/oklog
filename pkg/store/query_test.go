package store

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/oklog/prototype/pkg/fs"
)

func TestQueryEngineNaïve(t *testing.T)   { testQueryEngine(t, QueryEngineNaïve) }
func TestQueryEngineRipgrep(t *testing.T) { testQueryEngine(t, QueryEngineRipgrep) }

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
		t.Logf("FileLog at %s", root)
	}

	var (
		from, _   = time.Parse(time.RFC3339, "2017-01-01T15:52:30+01:00")
		to, _     = time.Parse(time.RFC3339, "2017-01-01T15:53:15+01:00")
		q         = "7A(0|1)"
		statsOnly = true
	)

	scatterQuery(t, logs, engine, from, to, q, statsOnly)
}

func scatterQuery(t *testing.T, logs []Log, engine QueryEngine, from, to time.Time, q string, statsOnly bool) {
	var (
		begin   = time.Now()
		results = make(chan QueryResult)
		errs    = make(chan error)
	)
	for _, log := range logs {
		go func(log Log) {
			if res, err := log.Query(engine, from, to, q, statsOnly); err != nil {
				errs <- err
			} else {
				results <- res
			}
		}(log)
	}
	for range logs {
		select {
		case res := <-results:
			t.Logf("Got result: %#v", res)
		case err := <-errs:
			t.Errorf("Got error: %v", err)
		}
	}
	t.Logf("Took %s", time.Since(begin))
}
