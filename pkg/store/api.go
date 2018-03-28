package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/oklog/oklog/pkg/cluster"
	"github.com/oklog/oklog/pkg/stream"
)

// These are the store API URL paths.
const (
	APIPathUserQuery      = "/query"
	APIPathInternalQuery  = "/_query"
	APIPathUserStream     = "/stream"
	APIPathInternalStream = "/_stream"
	APIPathReplicate      = "/replicate"
	APIPathClusterState   = "/_clusterstate"
)

// ClusterPeer models cluster.Peer.
type ClusterPeer interface {
	Current(cluster.PeerType) []string
	State() map[string]interface{}
}

// Doer models http.Client.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// API serves the store API.
type API struct {
	peer               ClusterPeer
	log                Log
	queryClient        Doer // should time out
	streamClient       Doer // should not time out
	streamQueries      *queryRegistry
	replicatedSegments prometheus.Counter
	replicatedBytes    prometheus.Counter
	duration           *prometheus.HistogramVec
	reporter           EventReporter
}

// NewAPI returns a usable API.
func NewAPI(
	peer ClusterPeer,
	log Log,
	queryClient, streamClient Doer,
	replicatedSegments, replicatedBytes prometheus.Counter,
	duration *prometheus.HistogramVec,
	reporter EventReporter,
) *API {
	return &API{
		peer:               peer,
		log:                log,
		queryClient:        queryClient,
		streamClient:       streamClient,
		streamQueries:      newQueryRegistry(),
		replicatedSegments: replicatedSegments,
		replicatedBytes:    replicatedBytes,
		duration:           duration,
		reporter:           reporter,
	}
}

// Close out the API, including the streaming query registry.
func (a *API) Close() error {
	return a.streamQueries.Close()
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

	method, path := r.Method, r.URL.Path
	switch {
	case method == "GET" && path == "/":
		r.URL.Path = APIPathUserQuery
		http.Redirect(w, r, r.URL.String(), http.StatusTemporaryRedirect)
	case (method == "GET" || method == "HEAD") && path == APIPathUserQuery:
		a.handleUserQuery(w, r)
	case (method == "GET" || method == "HEAD") && path == APIPathInternalQuery:
		a.handleInternalQuery(w, r)
	case method == "GET" && path == APIPathUserStream:
		a.handleUserStream(w, r)
	case method == "GET" && path == APIPathInternalStream:
		a.handleInternalStream(w, r)
	case method == "POST" && path == APIPathReplicate:
		a.handleReplicate(w, r)
	case method == "GET" && path == APIPathClusterState:
		a.handleClusterState(w, r)
	default:
		http.NotFound(w, r)
	}
}

type interceptingWriter struct {
	code int
	http.ResponseWriter
}

func (iw *interceptingWriter) WriteHeader(code int) {
	iw.code = code
	iw.ResponseWriter.WriteHeader(code)
}

func (iw *interceptingWriter) Flush() {
	// We always present as if we are a flusher, and squelch flush errors.
	// TODO(pb): de-hack-ify this
	if f, ok := iw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (a *API) handleUserQuery(w http.ResponseWriter, r *http.Request) {
	begin := time.Now()

	// Validate user input.
	var qp QueryParams
	if err := qp.DecodeFrom(r.URL, rangeRequired); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	members := a.peer.Current(cluster.PeerTypeStore)
	if len(members) <= 0 {
		// Very odd; we should at least find ourselves!
		http.Error(w, "no store nodes available", http.StatusServiceUnavailable)
		return
	}

	var requests []*http.Request
	for _, hostport := range members {
		// Copy original URL, to save all the query params, etc.
		u, err := url.Parse(r.URL.String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Fix the scheme, host, and path.
		// (These may be empty due to StripPrefix.)
		u.Scheme = "http"
		u.Host = hostport
		u.Path = fmt.Sprintf("store%s", APIPathInternalQuery)

		// Construct a new request.
		req, err := http.NewRequest(r.Method, u.String(), nil)
		if err != nil {
			err = errors.Wrapf(err, "constructing request for %s", hostport)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Execute that request later.
		requests = append(requests, req)
	}

	// Execute all requests concurrently.
	type response struct {
		resp *http.Response
		err  error
	}
	c := make(chan response, len(requests))
	for _, req := range requests {
		go func(req *http.Request) {
			resp, err := a.queryClient.Do(req)
			c <- response{resp, err}
		}(req)
	}

	// We'll collect responses into a single QueryResult.
	qr := QueryResult{Params: qp}

	// We'll merge all records in a single pass.
	var rcs []io.ReadCloser
	defer func() {
		// Don't leak if we need to make an early return.
		for i, rc := range rcs {
			if err := rc.Close(); err != nil {
				a.reporter.ReportEvent(Event{
					Op: "handleUserQuery", Error: err,
					Msg: fmt.Sprintf("Close of intermediate io.ReadCloser %d/%d failed", i+1, len(rcs)),
				})
			}
		}
	}()

	// Collect responses.
	responses := make([]response, cap(c))
	for i := 0; i < len(responses); i++ {
		responses[i] = <-c
	}
	for i, response := range responses {
		// Direct error, network problem?
		if response.err != nil {
			a.reporter.ReportEvent(Event{
				Op: "handleUserQuery", Error: response.err,
				Msg: fmt.Sprintf("gather query response from store %d/%d: total failure", i+1, len(responses)),
			})
			qr.ErrorCount++
			continue
		}

		// Non-2xx: internal server error or bad request?
		if (response.resp.StatusCode / 100) != 2 {
			buf, err := ioutil.ReadAll(response.resp.Body)
			if err != nil {
				buf = []byte(err.Error())
			}
			if len(buf) == 0 {
				buf = []byte("unknown")
			}
			response.resp.Body.Close()
			a.reporter.ReportEvent(Event{
				Op: "handleUserQuery", Error: fmt.Errorf(response.resp.Status),
				Msg: fmt.Sprintf("gather query response from store %d/%d: bad status (%s)", i+1, len(responses), strings.TrimSpace(string(buf))),
			})
			qr.ErrorCount++
			continue
		}

		// 206 is returned when a store knows it missed something.
		// For now, just emit an event, so it's possible to triage.
		if response.resp.StatusCode == http.StatusPartialContent {
			a.reporter.ReportEvent(Event{
				Op:  "handleUserQuery",
				Msg: fmt.Sprintf("gather query response from store %d/%d: partial content", i+1, len(responses)),
			})
			// TODO(pb): should we signal this in the QueryResult?
			// It shouldn't be an ErrorCount; maybe something else?
		}

		// Decode the individual result.
		var partialResult QueryResult
		if err := partialResult.DecodeFrom(response.resp); err != nil {
			err = errors.Wrap(err, "decoding partial result")
			a.reporter.ReportEvent(Event{
				Op: "handleUserQuery", Error: err,
				Msg: fmt.Sprintf("gather query response from store %d/%d: invalid response", i+1, len(responses)),
			})
			qr.ErrorCount++
			continue
		}

		// We do a single lazy merge of all records, at the end!
		// Extract the records ReadCloser, for later processing.
		rcs = append(rcs, partialResult.Records)
		partialResult.Records = nil

		// Merge everything else, though.
		if err := qr.Merge(partialResult); err != nil {
			err = errors.Wrap(err, "merging results")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Now bind all the partial ReadClosers together.
	mrc, err := newMergeReadCloser(rcs)
	if err != nil {
		err = errors.Wrap(err, "constructing merging reader")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	qr.Records = mrc // lazy reader
	rcs = nil        // don't double-close on return

	// Return!
	qr.Duration = time.Since(begin).String() // overwrite
	qr.EncodeTo(w)
}

func (a *API) handleInternalQuery(w http.ResponseWriter, r *http.Request) {
	var qp QueryParams
	if err := qp.DecodeFrom(r.URL, rangeRequired); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	statsOnly := false
	if r.Method == "HEAD" {
		statsOnly = true
	}

	result, err := a.log.Query(qp, statsOnly)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result.EncodeTo(w)
}

func (a *API) handleUserStream(w http.ResponseWriter, r *http.Request) {
	// Validate user input.
	var qp QueryParams
	if err := qp.DecodeFrom(r.URL, rangeNotRequired); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	window, err := time.ParseDuration(r.URL.Query().Get("window"))
	if err != nil {
		window = 3 * time.Second
	}
	if window < 100*time.Millisecond {
		window = 100 * time.Millisecond
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "can't stream to your client", http.StatusPreconditionFailed)
		return
	}

	peerFactory := func() []string {
		return a.peer.Current(cluster.PeerTypeStore)
	}

	// Use the special stream client, which doesn't time out.
	readCloserFactory := stream.HTTPReadCloserFactory(a.streamClient, func(addr string) string {
		// Copy original URL, to save all the query params, etc.
		u, err := url.Parse(r.URL.String())
		if err != nil {
			panic(err)
		}

		// Fix the scheme, host, and path.
		// (These may be empty due to StripPrefix.)
		u.Scheme = "http"
		u.Host = addr
		u.Path = fmt.Sprintf("store%s", APIPathInternalStream)

		// That's our internal stream URL.
		return u.String()
	})

	// Execute returns when the context is canceled.
	// We must close the raw chan, which we own.
	raw := make(chan []byte, 1024)
	go func() {
		stream.Execute(r.Context(), peerFactory, readCloserFactory, time.Sleep, time.NewTicker, raw)
		close(raw)
	}()

	// Deduplicate returns when the raw chan is closed.
	// We must close the deduplicated chan, which we own.
	deduplicated := make(chan []byte, 1024)
	go func() {
		stream.Deduplicate(raw, window, time.NewTicker, deduplicated)
		close(deduplicated)
	}()

	// Thus, we can range over the deduplicated chan.
	for record := range deduplicated {
		w.Write(record)
		w.Write([]byte{'\n'})
		flusher.Flush()
	}
}

func (a *API) handleInternalStream(w http.ResponseWriter, r *http.Request) {
	var qp QueryParams
	if err := qp.DecodeFrom(r.URL, rangeNotRequired); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "can't stream to your client", http.StatusPreconditionFailed)
		return
	}

	pass := recordFilterPlain([]byte(qp.Q))
	if qp.Regex {
		// QueryParams.DecodeFrom validated the regex.
		pass = recordFilterRegex(regexp.MustCompile(qp.Q))
	}

	// The records chan is closed when the context is canceled.
	records := a.streamQueries.Register(r.Context(), pass)

	// Thus, we range over the records chan.
	for record := range records {
		w.Write(record) // includes trailing newline
		flusher.Flush()
	}
}

func (a *API) handleReplicate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	segment, err := a.log.Create()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var buf bytes.Buffer
	lo, hi, n, err := teeRecords(r.Body, segment, &buf)
	if err != nil {
		segment.Delete()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else if n == 0 {
		segment.Delete()
		fmt.Fprintln(w, "No records")
		return
	}
	if err := segment.Close(lo, hi); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	go a.streamQueries.Match(buf.Bytes()) // TODO(pb): validate `go`
	a.replicatedSegments.Inc()
	a.replicatedBytes.Add(float64(n))
	fmt.Fprintln(w, "OK")
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

func teeRecords(src io.Reader, dst ...io.Writer) (lo, hi ulid.ULID, n int, err error) {
	var (
		first = true
		id    ulid.ULID
		w     = io.MultiWriter(dst...)
		s     = bufio.NewScanner(src)
	)
	s.Split(scanLinesPreserveNewline)
	for s.Scan() {
		// ULID and record-count accounting.
		line := s.Bytes()
		if err := id.UnmarshalText(line[:ulid.EncodedSize]); err != nil {
			return lo, hi, n, err
		}
		if first {
			lo, first = id, false
		}
		hi = id

		// Copying.
		n0, err := w.Write(line)
		if err != nil {
			return lo, hi, n, err
		}
		n += n0
	}
	return lo, hi, n, s.Err() // EOF yields nil
}
