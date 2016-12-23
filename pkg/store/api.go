package store

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/oklog/prototype/pkg/cluster"
)

const internalQueryPath = "/_query"

// API serves the store API.
type API struct {
	peer           *cluster.Peer
	log            Log
	duration       *prometheus.HistogramVec
	replicateBytes prometheus.Counter
}

// NewAPI returns a usable API.
func NewAPI(peer *cluster.Peer, log Log, duration *prometheus.HistogramVec, replicateBytes prometheus.Counter) *API {
	return &API{
		peer:           peer,
		log:            log,
		duration:       duration,
		replicateBytes: replicateBytes,
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
		r.URL.Path = "/query"
		http.Redirect(w, r, r.URL.String(), http.StatusTemporaryRedirect)
	case method == "GET" && path == "/query":
		a.handleUserQuery(w, r, false)
	case method == "HEAD" && path == "/query":
		a.handleUserQuery(w, r, true)
	case method == "GET" && path == internalQueryPath:
		a.handleInternalQuery(w, r, false)
	case method == "HEAD" && path == internalQueryPath:
		a.handleInternalQuery(w, r, true)
	case method == "POST" && path == "/replicate":
		a.handleReplicate(w, r)
	case method == "GET" && path == "/_clusterstate":
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

func (a *API) handleUserQuery(w http.ResponseWriter, r *http.Request, statsOnly bool) {
	members := a.peer.Current(cluster.PeerTypeStore)
	if len(members) <= 0 {
		// Very odd; we should at least find ourselves!
		http.Error(w, "no store nodes available", http.StatusServiceUnavailable)
		return
	}

	var (
		from = r.URL.Query().Get("from")
		to   = r.URL.Query().Get("to")
		q    = r.URL.Query().Get("q")
	)

	method := "GET"
	if statsOnly {
		method = "HEAD"
	}

	var requests []*http.Request
	for _, hostport := range members {
		urlStr := fmt.Sprintf(
			"http://%s/store/%s?from=%s&to=%s&q=%s",
			hostport, internalQueryPath,
			url.QueryEscape(from), url.QueryEscape(to), url.QueryEscape(q),
		)
		req, err := http.NewRequest(method, urlStr, nil)
		if err != nil {
			err = errors.Wrapf(err, "constructing URL for %s", hostport)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		requests = append(requests, req)
	}

	type response struct {
		resp *http.Response
		err  error
	}
	c := make(chan response, len(requests))
	for _, req := range requests {
		go func(req *http.Request) {
			resp, err := http.DefaultClient.Do(req)
			c <- response{resp, err}
		}(req)
	}

	responses := make([]response, cap(c))
	for i := 0; i < cap(c); i++ {
		responses[i] = <-c
	}
	result := QueryResult{
		From: from,
		To:   to,
		Q:    q,
	}
	for _, response := range responses {
		if response.err != nil {
			result.ErrorCount++ // TODO(pb): log or record error
			continue
		}
		if response.resp.StatusCode != http.StatusOK {
			result.ErrorCount++ // TODO(pb): log or record error
			continue
		}
		var partialResult QueryResult
		partialResult.DecodeFrom(response.resp)
		result.Merge(partialResult)
	}
	result.EncodeTo(w)
}

func (a *API) handleInternalQuery(w http.ResponseWriter, r *http.Request, statsOnly bool) {
	var (
		fromStr = r.URL.Query().Get("from")
		toStr   = r.URL.Query().Get("to")
		q       = r.URL.Query().Get("q")
	)
	from, err := time.Parse(time.RFC3339Nano, fromStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	to, err := time.Parse(time.RFC3339Nano, toStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := a.log.Query(from, to, q, statsOnly)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	result.EncodeTo(w)
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
	// TODO(pb): checksum
	if err := segment.Close(low, high); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.replicateBytes.Add(float64(n))
	fmt.Fprintln(w, "OK")
}

func (a *API) handleClusterState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(a.peer.State())
}
