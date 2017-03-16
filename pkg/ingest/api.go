package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

// These are the ingest API URL paths.
const (
	APIPathNext         = "/next"
	APIPathRead         = "/read"
	APIPathCommit       = "/commit"
	APIPathFailed       = "/failed"
	APIPathSegmentState = "/_segmentstate"
	APIPathClusterState = "/_clusterstate"
)

// API serves the ingest API.
type API struct {
	peer              ClusterPeer
	log               Log
	timeout           time.Duration
	pending           map[string]pendingSegment
	action            chan func()
	stop              chan chan struct{}
	failedSegments    prometheus.Counter
	committedSegments prometheus.Counter
	committedBytes    prometheus.Counter
	duration          *prometheus.HistogramVec
}

type pendingSegment struct {
	segment  ReadSegment
	deadline time.Time
	reading  bool
}

// ClusterPeer models cluster.Peer.
type ClusterPeer interface {
	State() map[string]interface{}
}

// NewAPI returns a usable ingest API.
func NewAPI(
	peer ClusterPeer,
	log Log,
	pendingSegmentTimeout time.Duration,
	failedSegments, committedSegments, committedBytes prometheus.Counter,
	duration *prometheus.HistogramVec,
) *API {
	a := &API{
		peer:              peer,
		log:               log,
		timeout:           pendingSegmentTimeout,
		pending:           map[string]pendingSegment{},
		action:            make(chan func()),
		stop:              make(chan chan struct{}),
		failedSegments:    failedSegments,
		committedSegments: committedSegments,
		committedBytes:    committedBytes,
		duration:          duration,
	}
	go a.loop()
	return a
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	iw := &interceptingWriter{http.StatusOK, w}
	w = iw
	defer func(begin time.Time) {
		a.duration.WithLabelValues(
			r.Method,
			r.URL.Path,
			strconv.Itoa(iw.code),
		).Observe(time.Since(begin).Seconds())
	}(time.Now())

	// Fuck all y'all's HN-frontpage-spamming zero-alloc muxers \m/(-_-)\m/
	method, path := r.Method, r.URL.Path
	switch {
	case method == "GET" && path == APIPathNext:
		a.handleNext(w, r)
	case method == "GET" && path == APIPathRead:
		a.handleRead(w, r)
	case method == "POST" && path == APIPathCommit:
		a.handleCommit(w, r)
	case method == "POST" && path == APIPathFailed:
		a.handleFailed(w, r)
	case method == "GET" && path == APIPathSegmentState:
		a.handleSegmentStatus(w, r)
	case method == "GET" && path == APIPathClusterState:
		a.handleClusterState(w, r)
	default:
		http.NotFound(w, r)
	}
}

// Stop terminates the API.
func (a *API) Stop() {
	c := make(chan struct{})
	a.stop <- c
	<-c
}

type interceptingWriter struct {
	code int
	http.ResponseWriter
}

func (iw *interceptingWriter) WriteHeader(code int) {
	iw.code = code
	iw.ResponseWriter.WriteHeader(code)
}

func (a *API) loop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case f := <-a.action:
			f()

		case now := <-ticker.C:
			a.clean(now)

		case c := <-a.stop:
			a.clean(time.Now().Add(10 * a.timeout)) // fail all pending segments
			close(c)
			return
		}
	}
}

// Fail any pending segment past its deadline,
// making it available for consumption again.
func (a *API) clean(now time.Time) {
	for id, s := range a.pending {
		if now.After(s.deadline) {
			if err := s.segment.Failed(); err != nil {
				panic(err)
			}
			delete(a.pending, id)
			a.failedSegments.Inc()
		}
	}
}

func (a *API) handleNext(w http.ResponseWriter, r *http.Request) {
	var (
		notFound   = make(chan struct{})
		otherError = make(chan error)
		nextID     = make(chan string)
	)
	a.action <- func() {
		s, err := a.log.Oldest()
		if err == ErrNoSegmentsAvailable {
			close(notFound)
			return
		}
		if err != nil {
			otherError <- err
			return
		}
		id := uuid.New()
		a.pending[id] = pendingSegment{s, time.Now().Add(a.timeout), false}
		nextID <- id
	}
	select {
	case <-notFound:
		http.NotFound(w, r)
	case err := <-otherError:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	case id := <-nextID:
		fmt.Fprint(w, id)
	}
}

func (a *API) handleRead(w http.ResponseWriter, r *http.Request) {
	var (
		segment  = make(chan ReadSegment)
		notFound = make(chan struct{})
		readOpen = make(chan struct{})
	)
	a.action <- func() {
		id := r.URL.Query().Get("id")
		s, ok := a.pending[id]
		if !ok {
			close(notFound)
			return
		}
		if s.reading {
			close(readOpen)
			return
		}
		s.reading = true
		a.pending[id] = s
		segment <- s.segment
	}
	select {
	case s := <-segment:
		io.Copy(w, s)
	case <-notFound:
		http.NotFound(w, r)
	case <-readOpen:
		http.Error(w, "another client is already reading this segment", http.StatusInternalServerError)
	}
}

func (a *API) handleCommit(w http.ResponseWriter, r *http.Request) {
	var (
		notFound  = make(chan struct{})
		notRead   = make(chan struct{})
		commitErr = make(chan error)
		commitOK  = make(chan int64)
	)
	a.action <- func() {
		id := r.URL.Query().Get("id")
		s, ok := a.pending[id]
		if !ok {
			close(notFound)
			return
		}
		if !s.reading {
			close(notRead)
			return
		}
		sz := s.segment.Size()
		if err := s.segment.Commit(); err != nil {
			commitErr <- err
			return
		}
		delete(a.pending, id)
		commitOK <- sz
	}
	select {
	case <-notFound:
		http.NotFound(w, r)
	case <-notRead:
		http.Error(w, "segment hasn't been read yet; can't commit", http.StatusPreconditionRequired)
	case err := <-commitErr:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	case n := <-commitOK:
		a.committedSegments.Inc()
		a.committedBytes.Add(float64(n))
		fmt.Fprint(w, "Commit OK")
	}
}

// Failed marks a pending segment failed.
func (a *API) handleFailed(w http.ResponseWriter, r *http.Request) {
	var (
		notFound  = make(chan struct{})
		failedErr = make(chan error)
		failedOK  = make(chan struct{})
	)
	a.action <- func() {
		id := r.URL.Query().Get("id")
		s, ok := a.pending[id]
		if !ok {
			close(notFound)
			return
		}
		if err := s.segment.Failed(); err != nil {
			failedErr <- err
			return
		}
		delete(a.pending, id)
		close(failedOK)
	}
	select {
	case <-notFound:
		http.NotFound(w, r)
	case err := <-failedErr:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	case <-failedOK:
		a.failedSegments.Inc()
		fmt.Fprint(w, "Failed OK")
	}
}

func (a *API) handleSegmentStatus(w http.ResponseWriter, r *http.Request) {
	status := make(chan string)
	a.action <- func() {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "%d pending\n", len(a.pending))
		for id, s := range a.pending {
			fmt.Fprintf(&buf, " %s: reading=%v deadline=%s\n", id, s.reading, s.deadline)
		}
		status <- buf.String()
	}
	fmt.Fprintf(w, <-status)
}

func (a *API) handleClusterState(w http.ResponseWriter, r *http.Request) {
	buf, err := json.MarshalIndent(a.peer.State(), "", "    ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(buf)
}
