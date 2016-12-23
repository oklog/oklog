package store

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
)

// QueryResult contains statistics about, and matching records for, a query.
type QueryResult struct {
	From string `json:"from"`
	To   string `json:"to"`
	Q    string `json:"q"`

	NodesQueried    int `json:"nodes_queried"`
	SegmentsQueried int `json:"segments_queried"`
	RecordsQueried  int `json:"records_queried"`
	RecordsMatched  int `json:"records_matched"`
	ErrorCount      int `json:"error_count,omitempty"`

	Records io.ReadCloser // TODO(pb): audit to ensure closing is valid throughout
}

// DecodeFrom decodes the QueryResult from the HTTP response.
func (qr *QueryResult) DecodeFrom(resp *http.Response) {
	qr.From = resp.Header.Get(httpHeaderFrom)
	qr.To = resp.Header.Get(httpHeaderTo)
	qr.Q = resp.Header.Get(httpHeaderQ)
	qr.NodesQueried, _ = strconv.Atoi(resp.Header.Get(httpHeaderNodesQueried))
	qr.SegmentsQueried, _ = strconv.Atoi(resp.Header.Get(httpHeaderSegmentsQueried))
	qr.RecordsQueried, _ = strconv.Atoi(resp.Header.Get(httpHeaderRecordsQueried))
	qr.RecordsMatched, _ = strconv.Atoi(resp.Header.Get(httpHeaderRecordsMatched))
	qr.Records = resp.Body
}

// EncodeTo encodes the QueryResult to the HTTP response writer.
func (qr *QueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderFrom, qr.From)
	w.Header().Set(httpHeaderTo, qr.To)
	w.Header().Set(httpHeaderQ, qr.Q)
	w.Header().Set(httpHeaderNodesQueried, strconv.Itoa(qr.NodesQueried))
	w.Header().Set(httpHeaderSegmentsQueried, strconv.Itoa(qr.SegmentsQueried))
	w.Header().Set(httpHeaderRecordsQueried, strconv.Itoa(qr.RecordsQueried))
	w.Header().Set(httpHeaderRecordsMatched, strconv.Itoa(qr.RecordsMatched))
	if qr.Records != nil {
		io.Copy(w, qr.Records)
		qr.Records.Close()
	}
}

// Merge the other QueryResult into this one.
func (qr *QueryResult) Merge(other QueryResult) {
	qr.NodesQueried += other.NodesQueried
	qr.SegmentsQueried += other.SegmentsQueried
	qr.RecordsQueried += other.RecordsQueried
	qr.RecordsMatched += other.RecordsMatched
	qr.ErrorCount += other.ErrorCount

	// TODO(pb): error handling during mergeRecords
	var buf bytes.Buffer
	switch {
	case qr.Records != nil && other.Records == nil:
		defer qr.Records.Close()
		mergeRecords(&buf, qr.Records)
	case qr.Records == nil && other.Records != nil:
		defer other.Records.Close()
		mergeRecords(&buf, other.Records)
	case qr.Records != nil && other.Records != nil:
		defer qr.Records.Close()
		defer other.Records.Close()
		mergeRecords(&buf, qr.Records, other.Records)
	}
	qr.Records = ioutil.NopCloser(&buf)
}

const (
	httpHeaderFrom            = "X-OKLog-From"
	httpHeaderTo              = "X-OKLog-To"
	httpHeaderQ               = "X-OKLog-Q"
	httpHeaderNodesQueried    = "X-OKLog-Nodes-Queried"
	httpHeaderSegmentsQueried = "X-OKLog-Segments-Queried"
	httpHeaderRecordsQueried  = "X-OKLog-Records-Queried"
	httpHeaderRecordsMatched  = "X-OKLog-Records-Matched"
)
