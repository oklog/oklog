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

	"github.com/oklog/oklog/pkg/cluster"
)

// These are the store API URL paths.
const (
	APIPathUserQuery     = "/query"
	APIPathInternalQuery = "/_query"
	APIPathReplicate     = "/replicate"
	APIPathClusterState  = "/_clusterstate"
)

// API serves the store API.
type API struct {
	peer               *cluster.Peer
	log                Log
	replicatedSegments prometheus.Counter
	replicatedBytes    prometheus.Counter
	duration           *prometheus.HistogramVec
}

// NewAPI returns a usable API.
func NewAPI(peer *cluster.Peer, log Log, replicatedSegments, replicatedBytes prometheus.Counter, duration *prometheus.HistogramVec) *API {
	return &API{
		peer:               peer,
		log:                log,
		replicatedSegments: replicatedSegments,
		replicatedBytes:    replicatedBytes,
		duration:           duration,
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
	case method == "GET" && path == APIPathUserQuery:
		a.handleUserQuery(w, r, false)
	case method == "HEAD" && path == APIPathUserQuery:
		a.handleUserQuery(w, r, true)
	case method == "GET" && path == APIPathInternalQuery:
		a.handleInternalQuery(w, r, false)
	case method == "HEAD" && path == APIPathInternalQuery:
		a.handleInternalQuery(w, r, true)
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

func (a *API) handleUserQuery(w http.ResponseWriter, r *http.Request, statsOnly bool) {
	begin := time.Now()

	members := a.peer.Current(cluster.PeerTypeStore)
	if len(members) <= 0 {
		// Very odd; we should at least find ourselves!
		http.Error(w, "no store nodes available", http.StatusServiceUnavailable)
		return
	}

	var (
		from     = r.URL.Query().Get("from")
		to       = r.URL.Query().Get("to")
		q        = r.URL.Query().Get("q")
		_, regex = r.URL.Query()["regex"]
	)

	method := "GET"
	if statsOnly {
		method = "HEAD"
	}

	var asRegex string
	if regex {
		asRegex = "&regex=true"
	}

	var requests []*http.Request
	for _, hostport := range members {
		urlStr := fmt.Sprintf(
			"http://%s/store%s?from=%s&to=%s&q=%s%s",
			hostport, APIPathInternalQuery,
			url.QueryEscape(from),
			url.QueryEscape(to),
			url.QueryEscape(q),
			asRegex,
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
			// TODO(pb): don't use http.DefaultClient
			resp, err := http.DefaultClient.Do(req)
			c <- response{resp, err}
		}(req)
	}

	responses := make([]response, cap(c))
	for i := 0; i < cap(c); i++ {
		responses[i] = <-c
	}
	result := QueryResult{
		From:  from,
		To:    to,
		Q:     q,
		Regex: regex,
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
		if err := result.Merge(partialResult); err != nil {
			err = errors.Wrap(err, "merging results")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	result.Duration = time.Since(begin).String()
	result.EncodeTo(w)
}

func (a *API) handleInternalQuery(w http.ResponseWriter, r *http.Request, statsOnly bool) {
	var (
		fromStr  = r.URL.Query().Get("from")
		toStr    = r.URL.Query().Get("to")
		q        = r.URL.Query().Get("q")
		_, regex = r.URL.Query()["regex"]
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
	result, err := a.log.Query(from, to, q, regex, statsOnly)
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
