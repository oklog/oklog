package store

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

// QueryParams defines all dimensions of a query.
// StatsOnly is implicit by the HTTP method.
type QueryParams struct {
	From  ulidOrTime `json:"from"`
	To    ulidOrTime `json:"to"`
	Q     string     `json:"q"`
	Regex bool       `json:"regex"`
	Topic string     `json:"topic"`
}

// DecodeFrom populates a QueryParams from a URL.
func (qp *QueryParams) DecodeFrom(u *url.URL, rb rangeBehavior) error {
	if err := qp.From.Parse(u.Query().Get("from")); err != nil && rb == rangeRequired {
		return errors.Wrap(err, "parsing 'from'")
	}
	if err := qp.To.Parse(u.Query().Get("to")); err != nil && rb == rangeRequired {
		return errors.Wrap(err, "parsing 'to'")
	}
	qp.Q = u.Query().Get("q")
	_, qp.Regex = u.Query()["regex"]
	qp.Topic = u.Query().Get("topic")

	if qp.Regex {
		if _, err := regexp.Compile(qp.Q); err != nil {
			return errors.Wrap(err, "compiling regex")
		}
	}

	return nil
}

type rangeBehavior int

const (
	rangeRequired rangeBehavior = iota
	rangeNotRequired
)

// ulidOrTime is how we interpret the From and To query params.
// Users may specify a valid ULID, or an RFC3339Nano timestamp.
// We prefer them in that order, and cross-populate the fields.
type ulidOrTime struct {
	ulid.ULID
	time.Time
}

// Parse a string, likely taken from a query param.
func (ut *ulidOrTime) Parse(s string) error {
	if id, err := ulid.Parse(s); err == nil {
		ut.ULID = id
		var (
			msec = id.Time()
			sec  = int64(msec / 1000)
			nsec = int64(msec%1000) * 1000000
		)
		ut.Time = time.Unix(sec, nsec).UTC()
		return nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		ut.ULID.SetEntropy(nil)
		// Pass t.UTC to mirror ulid.Now, which does the same.
		// (We use ulid.Now when generating ULIDs in the ingester.)
		ut.ULID.SetTime(ulid.Timestamp(t.UTC()))
		ut.Time = t
		return nil
	}
	return errors.Errorf("%s: can't parse as ULID or RFC3339 time", s)
}

// QueryResult contains statistics about, and matching records for, a query.
type QueryResult struct {
	Params QueryParams `json:"query"`

	NodesQueried    int    `json:"nodes_queried"`
	SegmentsQueried int    `json:"segments_queried"`
	MaxDataSetSize  int64  `json:"max_data_set_size"`
	ErrorCount      int    `json:"error_count,omitempty"`
	Duration        string `json:"duration"`

	Records io.ReadCloser // TODO(pb): audit to ensure closing is valid throughout
}

// EncodeTo encodes the QueryResult to the HTTP response writer.
// It also closes the records ReadCloser.
func (qr *QueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderFrom, qr.Params.From.Format(time.RFC3339))
	w.Header().Set(httpHeaderTo, qr.Params.To.Format(time.RFC3339))
	w.Header().Set(httpHeaderTopic, qr.Params.Topic)
	w.Header().Set(httpHeaderQ, qr.Params.Q)
	w.Header().Set(httpHeaderRegex, fmt.Sprint(qr.Params.Regex))

	w.Header().Set(httpHeaderNodesQueried, strconv.Itoa(qr.NodesQueried))
	w.Header().Set(httpHeaderSegmentsQueried, strconv.Itoa(qr.SegmentsQueried))
	w.Header().Set(httpHeaderMaxDataSetSize, strconv.FormatInt(qr.MaxDataSetSize, 10))
	w.Header().Set(httpHeaderErrorCount, strconv.Itoa(qr.ErrorCount))
	w.Header().Set(httpHeaderDuration, qr.Duration)

	if qr.ErrorCount > 0 {
		w.WriteHeader(http.StatusPartialContent)
	}

	if qr.Records != nil {
		// CopyBuffer can be useful for complex query pipelines.
		// TODO(pb): validate the 1MB buffer size with profiling
		buf := make([]byte, 1024*1024)
		io.CopyBuffer(w, qr.Records, buf)
		qr.Records.Close()
	}
}

// DecodeFrom decodes the QueryResult from the HTTP response.
func (qr *QueryResult) DecodeFrom(resp *http.Response) error {
	var err error
	if err = qr.Params.From.Parse(resp.Header.Get(httpHeaderFrom)); err != nil {
		return errors.Wrap(err, "from")
	}
	if err = qr.Params.To.Parse(resp.Header.Get(httpHeaderTo)); err != nil {
		return errors.Wrap(err, "to")
	}
	qr.Params.Topic = resp.Header.Get(httpHeaderTopic)
	qr.Params.Q = resp.Header.Get(httpHeaderQ)
	if qr.Params.Regex, err = strconv.ParseBool(resp.Header.Get(httpHeaderRegex)); err != nil {
		return errors.Wrap(err, "regex")
	}
	if qr.NodesQueried, err = strconv.Atoi(resp.Header.Get(httpHeaderNodesQueried)); err != nil {
		return errors.Wrap(err, "nodes queried")
	}
	if qr.SegmentsQueried, err = strconv.Atoi(resp.Header.Get(httpHeaderSegmentsQueried)); err != nil {
		return errors.Wrap(err, "segments queried")
	}
	if qr.MaxDataSetSize, err = strconv.ParseInt(resp.Header.Get(httpHeaderMaxDataSetSize), 10, 64); err != nil {
		return errors.Wrap(err, "max data set size")
	}
	if qr.ErrorCount, err = strconv.Atoi(resp.Header.Get(httpHeaderErrorCount)); err != nil {
		return errors.Wrap(err, "error count")
	}
	qr.Duration = resp.Header.Get(httpHeaderDuration)
	qr.Records = resp.Body
	return nil
}

// Merge the other QueryResult into this one.
func (qr *QueryResult) Merge(other QueryResult) error {
	// Union the simple integer types.
	qr.NodesQueried += other.NodesQueried
	qr.SegmentsQueried += other.SegmentsQueried
	if other.MaxDataSetSize > qr.MaxDataSetSize {
		qr.MaxDataSetSize = other.MaxDataSetSize
	}
	qr.ErrorCount += other.ErrorCount

	// Merge the record readers.
	// Both mergeRecords and multiCloser can handle nils.
	var buf bytes.Buffer
	_, _, _, err := mergeRecords(&buf, qr.Records, other.Records)
	multiCloser{qr.Records, other.Records}.Close()
	qr.Records = ioutil.NopCloser(&buf)

	// Done.
	return err
}

const (
	httpHeaderFrom            = "X-Oklog-From"
	httpHeaderTo              = "X-Oklog-To"
	httpHeaderTopic           = "X-Oklog-Topic"
	httpHeaderQ               = "X-Oklog-Q"
	httpHeaderRegex           = "X-Oklog-Regex"
	httpHeaderNodesQueried    = "X-Oklog-Nodes-Queried"
	httpHeaderSegmentsQueried = "X-Oklog-Segments-Queried"
	httpHeaderMaxDataSetSize  = "X-Oklog-Max-Data-Set-Size"
	httpHeaderErrorCount      = "X-Oklog-Error-Count"
	httpHeaderDuration        = "X-Oklog-Duration"
)
