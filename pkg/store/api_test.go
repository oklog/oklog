package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/oklog/oklog/pkg/cluster"
	"github.com/oklog/oklog/pkg/fs"
	"github.com/oklog/ulid"
)

func TestTeeRecords(t *testing.T) {
	t.Parallel()

	var records = []string{
		"01BB6RQR190000000000000000 Foo\n",
		"01BB6RRTB70000000000000000 Bar\n",
		"01BB6RT5GS0000000000000000 Baz\n",
		"01BB6RV5R00000000000000000 Quux\n",
	}
	var (
		src  = strings.NewReader(strings.Join(records, ""))
		dsts = []io.Writer{
			&bytes.Buffer{},
			&bytes.Buffer{},
			&bytes.Buffer{},
		}
	)

	lo, hi, n, err := teeRecords(src, dsts...)
	if err != nil {
		t.Fatal(err)
	}

	var totalSize int
	for _, s := range records {
		totalSize += len(s)
	}
	if want, have := totalSize, n; want != have {
		t.Errorf("n: want %d, have %d", want, have)
	}
	if want, have := ulid.MustParse("01BB6RQR190000000000000000"), lo; want != have {
		t.Errorf("lo: want %s, have %s", want.String(), have.String())
	}
	if want, have := ulid.MustParse("01BB6RV5R00000000000000000"), hi; want != have {
		t.Errorf("hi: want %s, have %s", want.String(), have.String())
	}
}

func TestAPIInternalQueryFromULID(t *testing.T) {
	t.Parallel()

	a, err := newFixtureAPI(t)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", fmt.Sprintf(
		"%s?from=%s&to=%s",
		APIPathInternalQuery,
		"01BB6RT5GR0000000000000000", // Just a bit before C
		"01BB6RWTY70000000000000000", // Just a bit after G
	), nil)
	a.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("Query failed: HTTP %d: %s", w.Code, strings.TrimSpace(w.Body.String()))
	}
	if want, have := recordC+recordD+recordE+recordF+recordG, w.Body.String(); want != have {
		t.Errorf("Results: want:\n%s\nhave:\n%s", want, have)
	}
}

var (
	recordA  = "01BB6RQR190000000000000000 A 2017-03-14T16:59:40.585457189+01:00\n"
	recordB  = "01BB6RRTB70000000000000000 B 2017-03-14T17:00:15.719316824+01:00\n"
	recordC  = "01BB6RT5GS0000000000000000 C 2017-03-14T17:00:59.929816245+01:00\n"
	recordD  = "01BB6RV5R00000000000000000 D 2017-03-14T17:01:32.928453488+01:00\n"
	recordE  = "01BB6RVR490000000000000000 E 2017-03-14T17:01:51.753613999+01:00\n"
	recordF  = "01BB6RW6C60000000000000000 F 2017-03-14T17:02:06.342946304+01:00\n"
	recordG  = "01BB6RWTY60000000000000000 G 2017-03-14T17:02:27.398068977+01:00\n"
	recordH  = "01BB6RX9D30000000000000000 H 2017-03-14T17:02:42.211235645+01:00\n"
	recordI  = "01BB6RXQ090000000000000000 I 2017-03-14T17:02:56.137528308+01:00\n"
	segments = []string{
		recordA + recordB + recordC, // first segment
		recordD + recordE + recordF, // second segment
		recordG + recordH + recordI, // third segment
	}
)

func newFixtureAPI(t *testing.T) (*API, error) {
	// Loggers.
	var (
		baseLogger  = log.NewLogfmtLogger(os.Stderr)
		logReporter = LogReporter{log.With(baseLogger, "component", "FileLog")}
		apiReporter = LogReporter{log.With(baseLogger, "component", "API")}
	)

	// Construct a virtual file log.
	filesys := fs.NewVirtualFilesystem()
	filelog, err := NewFileLog(filesys, "/", 10240, 1024, logReporter)
	if err != nil {
		return nil, err
	}

	// Build an API around that file log.
	var (
		peer               = mockClusterPeer{}
		queryClient        = mockDoer{}
		streamClient       = mockDoer{}
		replicatedSegments = prometheus.NewCounter(prometheus.CounterOpts{})
		replicatedBytes    = prometheus.NewCounter(prometheus.CounterOpts{})
		duration           = prometheus.NewHistogramVec(prometheus.HistogramOpts{}, []string{"method", "path", "status_code"})
		a                  = NewAPI(peer, filelog, queryClient, streamClient, replicatedSegments, replicatedBytes, duration, apiReporter)
	)

	// Populate the store via the replicate API.
	for i, segment := range segments {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", APIPathReplicate, strings.NewReader(segment))
		a.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			a.Close()
			return nil, fmt.Errorf("Replicate %d failed: HTTP %d (%s)", i, w.Code, strings.TrimSpace(w.Body.String()))
		}
	}

	// Debug: dump the filesys.
	filesys.Walk("/", func(path string, info os.FileInfo, err error) error {
		t.Logf("Debug: Walk: %s (%dB)", path, info.Size())
		return nil
	})

	// Return the populated API.
	return a, nil
}

type mockClusterPeer struct{}

func (mockClusterPeer) Current(cluster.PeerType) []string { return []string{} }
func (mockClusterPeer) State() map[string]interface{}     { return map[string]interface{}{} }

type mockDoer struct{}

func (mockDoer) Do(*http.Request) (*http.Response, error) { return nil, errors.New("not implemented") }
