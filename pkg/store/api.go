package store

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	level "github.com/go-kit/kit/log/experimental_level"
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

// API serves the store API.
type API struct {
	peer               *cluster.Peer
	log                Log
	client             *http.Client
	replicatedSegments prometheus.Counter
	replicatedBytes    prometheus.Counter
	duration           *prometheus.HistogramVec
	logger             log.Logger
}

// NewAPI returns a usable API.
func NewAPI(
	peer *cluster.Peer,
	log Log,
	client *http.Client,
	replicatedSegments, replicatedBytes prometheus.Counter,
	duration *prometheus.HistogramVec,
	logger log.Logger,
) *API {
	return &API{
		peer:               peer,
		log:                log,
		client:             client,
		replicatedSegments: replicatedSegments,
		replicatedBytes:    replicatedBytes,
		duration:           duration,
		logger:             logger,
	}
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

func (a *API) handleUserQuery(w http.ResponseWriter, r *http.Request) {
	begin := time.Now()

	// Validate user input.
	var qp QueryParams
	if err := qp.DecodeFrom(r.URL); err != nil {
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
			resp, err := a.client.Do(req)
			c <- response{resp, err}
		}(req)
	}

	// We'll collect responses into a single QueryResult.
	qr := QueryResult{Params: qp}

	// We'll merge all records in a single pass.
	var readClosers []io.ReadCloser
	defer func() {
		// Don't leak if we need to make an early return.
		for _, rc := range readClosers {
			rc.Close()
		}
	}()

	// Collect responses.
	responses := make([]response, cap(c))
	for i := 0; i < len(responses); i++ {
		responses[i] = <-c
	}
	for _, response := range responses {
		// Direct error, network problem?
		if response.err != nil {
			level.Error(a.logger).Log("during", "query_gather", "err", response.err)
			qr.ErrorCount++
			continue
		}

		// Non-200, bad parameters or internal server error?
		if response.resp.StatusCode != http.StatusOK {
			buf, err := ioutil.ReadAll(response.resp.Body)
			if err != nil {
				buf = []byte(err.Error())
			}
			if len(buf) == 0 {
				buf = []byte("unknown")
			}
			response.resp.Body.Close()
			level.Error(a.logger).Log("during", "query_gather", "status_code", response.resp.StatusCode, "err", strings.TrimSpace(string(buf)))
			qr.ErrorCount++
			continue
		}

		// Decode the individual result.
		var partialResult QueryResult
		if err := partialResult.DecodeFrom(response.resp); err != nil {
			err = errors.Wrap(err, "decoding partial result")
			level.Error(a.logger).Log("during", "query_gather", "err", err)
			qr.ErrorCount++
			continue
		}

		// We do a single lazy merge of all records, at the end!
		// Extract the records ReadCloser, for later processing.
		readClosers = append(readClosers, partialResult.Records)
		partialResult.Records = nil

		// Merge everything else, though.
		if err := qr.Merge(partialResult); err != nil {
			err = errors.Wrap(err, "merging results")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Now bind all the partial ReadClosers together.
	mrc, err := newMergeReadCloser(readClosers)
	if err != nil {
		err = errors.Wrap(err, "constructing merging reader")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	qr.Records = mrc                // lazy reader
	readClosers = []io.ReadCloser{} // don't double-close on return

	// Return!
	qr.Duration = time.Since(begin).String() // overwrite
	qr.EncodeTo(w)
}

func (a *API) handleInternalQuery(w http.ResponseWriter, r *http.Request) {
	var qp QueryParams
	if err := qp.DecodeFrom(r.URL); err != nil {
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
	if err := qp.DecodeFrom(r.URL); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "can't stream to your client", http.StatusPreconditionFailed)
		return
	}

	peerFactory := func() []string {
		return a.peer.Current(cluster.PeerTypeStore)
	}

	readerFactory := stream.HTTPReaderFactory(a.client, func(addr string) string {
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

	records := make(chan []byte)
	go stream.Execute(
		r.Context(),
		peerFactory,
		readerFactory,
		records,
		time.Sleep,
		time.NewTicker,
	)

	for {
		select {
		case record := <-records:
			w.Write(append(record, '\n'))
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (a *API) handleInternalStream(w http.ResponseWriter, r *http.Request) {
	var qp QueryParams
	if err := qp.DecodeFrom(r.URL); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "can't stream to your client", http.StatusPreconditionFailed)
		return
	}

	records := a.log.Stream(r.Context(), qp)

	for {
		select {
		case record := <-records:
			fmt.Fprintf(w, "%s\n", record)
			flusher.Flush()

		case <-r.Context().Done():
			// Context cancelation is transitive.
			// We just need to exit.
			return
		}
	}
}

func (a *API) handleReplicate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	segment, err := a.log.Create()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	low, high, n, err := mergeRecords(segment, r.Body)
	if err != nil {
		segment.Delete()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else if n == 0 {
		segment.Delete()
		fmt.Fprintln(w, "No records")
		return
	}
	if err := segment.Close(low, high); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
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
